package com.zeotap.zeoflow.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.sink.spark.writer.SparkWriter
import com.zeotap.data.io.source.spark.loader.SparkLoader
import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.expectations.column.helper.ColumnExpectationUtils.ColumnExpectationOps
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.zeoflow.common.constructs.Production
import com.zeotap.zeoflow.common.dsl.FlowDSLHelper.assertExpectation
import com.zeotap.zeoflow.common.test.helpers.DataFrameUtils._
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}
import com.zeotap.zeoflow.spark.interpreters.SparkInterpreters._
import com.zeotap.zeoflow.spark.test.processor.TestProcessor2
import com.zeotap.zeoflow.spark.types.{SparkSQLQueryProcessor, SparkSink, SparkSource, SparkUDF}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.Assert.assertNotNull
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
        Row("2020-05-13","Martos","IAB6","Male","35","44", "1628623500", "ESP"),
        Row("2020-05-13","Martos","IAB6","Male","35","44", "1628623500", "POL"),
        Row("2020-05-13","Martos","IAB6","Male","35","44", "1628623500", "FIN"),
        Row("2020-05-13","Martos","IAB6","Male","35","44", "1628623500", "SWE")
      )),
      StructType(sampleDataFrameSchema)
    )

     val minAgeExpectation: ColumnExpectation = ColumnExpectation("MinAge").addAgeExpectation().oneOf(List("18", "25", "35", "45", "55", "65"))
     val maxAgeExpectation: ColumnExpectation = ColumnExpectation("MaxAge").addAgeExpectation().oneOf(List("24", "34", "44", "54", "64", "99"))
     val interestIABExpectation: ColumnExpectation = ColumnExpectation("Interest_IAB_preprocess").addDynamicColumnExpectation()
     val genderExpectation: ColumnExpectation = ColumnExpectation("Gender").addGenderExpectation(List()).mayHaveNullLiteral
     val cityExpectation: ColumnExpectation = ColumnExpectation("city").mayHaveNullLiteral
     val createdTsExpectation: ColumnExpectation = ColumnExpectation("CREATED_TS_raw").hasValidLength(List(10)).addTimestampExpectation()
     val timestampExpectation: ColumnExpectation = ColumnExpectation("timestamp").hasValidLength(List(10)).addTimestampExpectation()
     val countryExpectation:ColumnExpectation = ColumnExpectation("country_code_iso3").hasValidLength(List(3)).oneOf(List("FRA", "GBR", "ESP", "DEU")).noneOf(List("FIN", "SWE", "POL")).addMandatoryColumnExpectation()

     val sampleDataPartner_output_data_expectation: Array[ColumnExpectation] = Array(
       timestampExpectation,
       cityExpectation,
       interestIABExpectation,
       genderExpectation,
       minAgeExpectation,
       maxAgeExpectation,
       createdTsExpectation,
       countryExpectation
     )

     val inputDataFrame: Map[String, DataFrame] = Map("OutputTable" -> sampleDataFrame)
     val inputColumnDSL: Map[String, ColumnDSL] = Map("OutputTable" -> ColumnDSL(sampleDataPartner_output_data_expectation: _*))
     val actualOutput = assertExpectation(inputColumnDSL)
                        .foldMap[SparkFlow](sparkFlowInterpreter)
                        .run(inputDataFrame).value._2
                        .asInstanceOf[Map[String, Map[String, ExpectationResult]]]

     assertNotNull(actualOutput)
  }
}
