package com.zeotap.zeoflow.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.sink.spark.writer.SparkWriter
import com.zeotap.data.io.source.spark.loader.SparkLoader
import com.zeotap.expectations.column.dsl.ColumnDSL
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.zeoflow.common.constructs.Production
import com.zeotap.zeoflow.common.dsl.FlowDSLHelper.runColumnExpectation
import com.zeotap.zeoflow.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.zeoflow.common.test.helpers.SampleDpAssertionDefinition
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}
import com.zeotap.zeoflow.spark.interpreters.SparkInterpreters._
import com.zeotap.zeoflow.spark.test.processor.TestProcessor2
import com.zeotap.zeoflow.spark.types.{SparkSQLQueryProcessor, SparkSink, SparkSource, SparkUDF}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import java.io.File

class SparkProductionE2EFlowTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  override def beforeAll(): Unit = {
    super.beforeAll()
    val schema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val dataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema)
    )

    dataFrame.write.format("avro").save("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(new File("src/test/resources/custom-input-format"))
  }

  override def afterEach(): Unit = {
    super.afterEach()
    FileUtils.deleteQuietly(new File("src/test/resources/custom-output-format"))
  }

  test("e2eFlowWithoutUDFsTest") {
    implicit val sparkSession: SparkSession = spark

    val sources: List[Source[DataFrame]] = List(
      SparkSource("dataFrame", SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"))
    )

    val transformations: List[Transformation[DataFrame]] = List(
      SparkSQLQueryProcessor("dataFrame2", "select *, 'abc' as newCol from dataFrame"),
      SparkSQLQueryProcessor("dataFrame3", "select DeviceId, newCol from dataFrame2"),
      SparkSQLQueryProcessor("dataFrame4", "select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2"),
      new TestProcessor2
    )

    val sinks: List[Sink[DataFrame]] = List(
      SparkSink("dataFrame5", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1")),
      SparkSink("dataFrame6", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2"))
    )

    Production.e2eFlow(sources, List(), transformations, sinks).foldMap[SparkFlow](sparkFlowInterpreter).run(Map()).value._2

    val expectedSchema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, true),
      StructField("dummy", StringType, true)
    )

    val expectedDataFrame1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc",null),
        Row(1,"2","India","1504679359","abc",null),
        Row(1,"3","Spain","1504679459","abc",null),
        Row(1,"4","India","1504679659","abc",null)
      )),
      StructType(expectedSchema1)
    )

    val expectedSchema2 = List(
      StructField("DeviceId", StringType, true),
      StructField("newCol", StringType, true),
      StructField("random", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("1","abc",null),
        Row("2","abc",null),
        Row("3","abc",null),
        Row("4","abc",null)
      )),
      StructType(expectedSchema2)
    )

    val actualDataFrame1 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1")
    val actualDataFrame2 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2")

    assertDataFrameEquality(expectedDataFrame1, actualDataFrame1, "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, actualDataFrame2, "DeviceId")
  }

  test("e2eFlowTest") {
    implicit val sparkSession: SparkSession = spark

    val sources: List[Source[DataFrame]] = List(
      SparkSource("dataFrame", SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"))
    )

    val dummyUDF: UserDefinedFunction = udf((column: String) => s"$column ABC")
    val sumUDF: UserDefinedFunction = udf((column1: String, column2: String) => s"$column1|$column2")

    val udfs: List[FlowUDF[Unit]] = List(
      SparkUDF("dummyFunc", dummyUDF),
      SparkUDF("sumFunc", sumUDF)
    )

    val transformations: List[Transformation[DataFrame]] = List(
      SparkSQLQueryProcessor("dataFrame2", "select *, 'abc' as newCol from dataFrame"),
      SparkSQLQueryProcessor("dataFrame3", "select DeviceId, newCol from dataFrame2"),
      SparkSQLQueryProcessor("dataFrame4", "select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2"),
      new TestProcessor2,
      SparkSQLQueryProcessor("dataFrame7", "select *, dummyFunc(newCol) as newCol2, sumFunc(newCol, random) as newCol3 from dataFrame6")
    )

    val sinks: List[Sink[DataFrame]] = List(
      SparkSink("dataFrame5", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1")),
      SparkSink("dataFrame6", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2")),
      SparkSink("dataFrame7", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path3"))
    )

    Production.e2eFlow(sources, udfs, transformations, sinks).foldMap[SparkFlow](sparkFlowInterpreter).run(Map()).value._2

    val expectedSchema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, true),
      StructField("dummy", StringType, true)
    )

    val expectedDataFrame1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc",null),
        Row(1,"2","India","1504679359","abc",null),
        Row(1,"3","Spain","1504679459","abc",null),
        Row(1,"4","India","1504679659","abc",null)
      )),
      StructType(expectedSchema1)
    )

    val expectedSchema2 = List(
      StructField("DeviceId", StringType, true),
      StructField("newCol", StringType, true),
      StructField("random", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("1","abc",null),
        Row("2","abc",null),
        Row("3","abc",null),
        Row("4","abc",null)
      )),
      StructType(expectedSchema2)
    )

    val expectedSchema3 = List(
      StructField("DeviceId", StringType, true),
      StructField("newCol", StringType, true),
      StructField("random", StringType, true),
      StructField("newCol2", StringType, true),
      StructField("newCol3", StringType, true)
    )

    val expectedDataFrame3 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("1","abc",null,"abc ABC","abc|null"),
        Row("2","abc",null,"abc ABC","abc|null"),
        Row("3","abc",null,"abc ABC","abc|null"),
        Row("4","abc",null,"abc ABC","abc|null")
      )),
      StructType(expectedSchema3)
    )

    val actualDataFrame1 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1")
    val actualDataFrame2 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2")
    val actualDataFrame3 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path3")

    assertDataFrameEquality(expectedDataFrame1, actualDataFrame1, "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, actualDataFrame2, "DeviceId")
    assertDataFrameEquality(expectedDataFrame3, actualDataFrame3, "DeviceId")
  }

  test("Assert Expectation Test") {

    implicit val sparkSession: SparkSession = spark

    val sampleDataFrameSchema = List(
      StructField("timestamp", StringType, true),
      StructField("city", StringType, true),
      StructField("Interest_IAB_preprocess", StringType, true),
      StructField("Gender", StringType, true),
      StructField("MinAge", StringType, true),
      StructField("MaxAge", StringType, true),
      StructField("CREATED_TS_raw", StringType, true),
      StructField("country_code_iso3", StringType, true)
    )

    val sampleDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("2020-05-13","Berlin","IAB6","Male","35","44", "1628623500", "ESP"),
        Row("2021-07-30","Motril","IAB1_2,IAB19","Female","18","24","1627409640", "DEU"),
        Row("2021-08-11","Paris","IAB6,IAB1_2","Male","25","34", "1628623500", "GBR"),
        Row("2020-05-13", null, " ", null, "35", null, "1628623500", "ESP"),
        Row("2021-07-30","Motril","IAB1_2,IAB19","Female","18","24", "1627409640", "DEU"),
        Row("2020-05-13", null, " ", null, "35", null, "1628623500", "ESP"),
        Row("2021-07-30","Motril","IAB1_2,IAB19","Female","18","24", "1627409640", "DEU"),
        Row("2021-08-11","Paris","IAB6,IAB1_2","Male","25","34", "1628623500", "GBR"),
        Row("2021-07-30","Motril", null, "Female","18","24", "1627409640", "DEU"),
        Row("2020-05-13","Martos","IAB6","Male","35","44", "1628623500", "ESP")
      )),
      StructType(sampleDataFrameSchema)
    )

    val inputDataFrame: Map[String, DataFrame] = Map("preprocessDF" -> sampleDataFrame)
    val sampleDP = new SampleDpAssertionDefinition()
    val inputColumnDSL: Map[String, ColumnDSL] = sampleDP.getColumnDSL("eu", "profile")

    val actualOutput: Map[String, Map[String, ExpectationResult]] = runColumnExpectation(inputColumnDSL).foldMap[SparkFlow](sparkFlowInterpreter).run(inputDataFrame).value._2.asInstanceOf[Map[String, Map[String, ExpectationResult]]]

    val expectedOutput = "Map(preprocessDF -> " +
      "Map(city -> WriterT((Chain(Right(MayHaveNullValueMetric(true,Some(city column may have null values but do not have empty string or white spaces as values in DataFrame),None,2,0))),Some(true))), " +
      "timestamp -> WriterT((Chain(Right(TsFormatMetric(false,None,Some(Some timestamp Column Values have invalid timestamp format),0)), Right(NonNullMetric(true,Some(timestamp column have only non-null values),None,0)), Right(DefaultMetric(true,Some(timestamp column has datatype as StringType),None)), Right(validLengthCheckMetric(true,Some(timestamp column have cookies of Valid Length),None,10,0,10))),Some(false))), " +
      "MinAge -> WriterT((Chain(Right(ValidEntryMetric(true,Some(MinAge column has one or more valid entries as mentioned in valid entry list),None,10)), Right(AgeBucketMetric(true,Some(MinAge column values lie within valid age range),None,10,0,0,10)), Right(MayHaveNullValueMetric(true,Some(MinAge column may have null values but do not have empty string or white spaces as values in DataFrame),None,0,0)), Right(DefaultMetric(true,Some(MinAge column has datatype as StringType),None))),Some(true))), " +
      "CREATED_TS_raw -> WriterT((Chain(Right(TsFormatMetric(true,Some(CREATED_TS_raw column values matches predefined timestamp formats),None,10)), Right(NonNullMetric(true,Some(CREATED_TS_raw column have only non-null values),None,0)), Right(DefaultMetric(true,Some(CREATED_TS_raw column has datatype as StringType),None)), Right(validLengthCheckMetric(true,Some(CREATED_TS_raw column have cookies of Valid Length),None,10,0,10))),Some(true))), " +
      "country_code_iso3 -> WriterT((Chain(Right(NonNullMetric(true,Some(country_code_iso3 column have only non-null values),None,0)), Right(DefaultMetric(true,Some(country_code_iso3 column has datatype as StringType),None)), Right(ValidEntryMetric(true,Some(country_code_iso3 column has one or more valid entries as mentioned in valid entry list),None,10)), Right(validLengthCheckMetric(true,Some(country_code_iso3 column have cookies of Valid Length),None,10,0,10))),Some(true))), " +
      "MaxAge -> WriterT((Chain(Right(ValidEntryMetric(true,Some(MaxAge column has one or more valid entries as mentioned in valid entry list),None,8)), Right(AgeBucketMetric(true,Some(MaxAge column values lie within valid age range),None,8,2,0,10)), Right(MayHaveNullValueMetric(true,Some(MaxAge column may have null values but do not have empty string or white spaces as values in DataFrame),None,2,0)), Right(DefaultMetric(true,Some(MaxAge column has datatype as StringType),None))),Some(true))), " +
      "Interest_IAB_preprocess -> WriterT((Chain(Right(CommaSeparatedMetric(false,None,Some(Some Interest_IAB_preprocess Column Values does not have proper comma separated values),7,9)), Right(MayHaveNullValueMetric(false,None,Some(Interest_IAB_preprocess column have empty String or white spaces or null as String value in the DataFrame),1,2)), Right(DefaultMetric(true,Some(Interest_IAB_preprocess column has datatype as StringType),None))),Some(false))), " +
      "Gender -> WriterT((Chain(Right(MayHaveNullValueMetric(true,Some(Gender column may have null values but do not have empty string or white spaces as values in DataFrame),None,2,0)), Right(InValidEntryMetric(true,Some(Gender column does not have any invalid entry in data frame),None,0)), Right(ValidEntryMetric(true,Some(Gender column has one or more valid entries as mentioned in valid entry list),None,8)), Right(MayHaveNullValueMetric(true,Some(Gender column may have null values but do not have empty string or white spaces as values in DataFrame),None,2,0)), Right(DefaultMetric(true,Some(Gender column has datatype as StringType),None))),Some(true)))))"

    assertResult(expectedOutput)(actualOutput.toString())
  }

  /*test("e2e FlowDSL Test with Assertions") {

    implicit val sparkSession: SparkSession = spark

    val sources: List[Source[DataFrame]] = List(
      SparkSource("dataFrame", SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"))
    )

    val dummyUDF: UserDefinedFunction = udf((column: String) => s"$column".concat("_sampleValue"))

    val udfs: List[FlowUDF[Unit]] = List(
      SparkUDF("dummyFunc", dummyUDF)
    )

    val transformations: List[Transformation[DataFrame]] = List(
      SparkSQLQueryProcessor("dataFrame2", "select *, 'abc' as newCol from dataFrame"),
      SparkSQLQueryProcessor("preprocessDF", "select *, dummyFunc(newCol) as newCol2 from dataFrame2")
    )

    val sampleDP = new SampleDpAssertionDefinition()
    val inputColumnDSL: Map[String, ColumnDSL] = sampleDP.getColumnDSL("in", "profile")

    val sinks: List[Sink[DataFrame]] = List(
      SparkSink("dataFrame2", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1")),
      SparkSink("preprocessDF", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2"))
    )

    Production.e2eFlowWithExpectations(sources, udfs, transformations, inputColumnDSL).foldMap[SparkFlow](sparkFlowInterpreter).run(Map()).value._2

    val expectedSchema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, true)
    )

    val expectedDataFrame1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc"),
        Row(1,"2","India","1504679359","abc"),
        Row(1,"3","Spain","1504679459","abc"),
        Row(1,"4","India","1504679659","abc")
      )),
      StructType(expectedSchema1)
    )

    val expectedSchema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, true),
      StructField("newCol2", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc", "abc_sampleValue"),
        Row(1,"2","India","1504679359","abc", "abc_sampleValue"),
        Row(1,"3","Spain","1504679459","abc", "abc_sampleValue"),
        Row(1,"4","India","1504679659","abc", "abc_sampleValue")
      )),
      StructType(expectedSchema2)
    )

    val actualDataFrame1 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1")
    val actualDataFrame2 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2")

    assertDataFrameEquality(expectedDataFrame1, actualDataFrame1, "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, actualDataFrame2, "DeviceId")

  }*/

}
