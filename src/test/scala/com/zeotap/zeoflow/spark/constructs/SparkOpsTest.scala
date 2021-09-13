package com.zeotap.zeoflow.spark.constructs

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.sink.spark.writer.SparkWriter
import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}
import com.zeotap.zeoflow.spark.test.processor.{TestProcessor, TestProcessor2}
import com.zeotap.zeoflow.spark.types.{SparkSQLQueryProcessor, SparkSink, SparkSource, SparkUDF}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite

import java.io.File

class SparkOpsTest extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(new File("src/test/resources/custom-input-format"))
    FileUtils.deleteQuietly(new File("src/test/resources/custom-output-format"))
  }

  test("loadSourcesTest") {
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

    dataFrame.write.format("avro").save("src/test/resources/custom-input-format/yr=2021/mon=09/dt=08")

    val sources: List[Source[DataFrame]] = List(
      SparkSource("dataFrame", SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=09/dt=08"))(spark)
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    val sourcesMap: Map[String, DataFrame] = spark.loadSources(sources)

    assertDataFrameEquality(dataFrame, sourcesMap("dataFrame"), "DeviceId")
  }

  test("loadUDFsTest") {
    val udfs: List[FlowUDF] = List(
      SparkUDF("twiceFunc", udf((column: String) => s"$column$column"))(spark),
      SparkUDF("thriceFunc", udf((column: String) => s"$column$column$column"))(spark)
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    spark.loadUserDefinedFunctions(udfs)

    val loadedUDFs = spark.sql("show user functions").collect.map(_.toString()).sorted
    assert(loadedUDFs.length == 2)
    assert(loadedUDFs(0).equalsIgnoreCase("[thriceFunc]"))
    assert(loadedUDFs(1).equalsIgnoreCase("[twiceFunc]"))

    val schema = List(
      StructField("country", StringType, true)
    )

    val dataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("India"),
        Row("India"),
        Row("Spain"),
        Row("India")
      )),
      StructType(schema)
    )

    val expectedSchema = List(
      StructField("country", StringType, true),
      StructField("country_twice", StringType, true),
      StructField("country_thrice", StringType, true)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("India","IndiaIndia","IndiaIndiaIndia"),
        Row("India","IndiaIndia","IndiaIndiaIndia"),
        Row("Spain","SpainSpain","SpainSpainSpain"),
        Row("India","IndiaIndia","IndiaIndiaIndia")
      )),
      StructType(expectedSchema)
    )

    dataFrame.createOrReplaceTempView("dataFrame")
    val actualDataFrame = spark.sql("select country, twiceFunc(country) as country_twice, thriceFunc(country) as country_thrice from dataFrame")
    assertDataFrameEquality(expectedDataFrame, actualDataFrame, "country")
  }

  test("runSingleQueryTransformationTest") {
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

    val transformations: List[Transformation[DataFrame]] = List(
      SparkSQLQueryProcessor("dataFrame2", "select *, 'abc' as newCol from dataFrame")(spark)
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    val transformationOutput = spark.runTransformations(Map("dataFrame" -> dataFrame), transformations)

    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc"),
        Row(1,"2","India","1504679359","abc"),
        Row(1,"3","Spain","1504679459","abc"),
        Row(1,"4","India","1504679659","abc")
      )),
      StructType(expectedSchema)
    )

    assertDataFrameEquality(expectedDataFrame, transformationOutput("dataFrame2"), "DeviceId")
  }

  test("runMultipleQueryTransformationsTest") {
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

    val transformations: List[Transformation[DataFrame]] = List(
      SparkSQLQueryProcessor("dataFrame2", "select *, 'abc' as newCol from dataFrame")(spark),
      SparkSQLQueryProcessor("dataFrame3", "select *, 'def' as newCol2 from dataFrame2")(spark)
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    val transformationOutput = spark.runTransformations(Map("dataFrame" -> dataFrame), transformations)

    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false),
      StructField("newCol2", StringType, false)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc","def"),
        Row(1,"2","India","1504679359","abc","def"),
        Row(1,"3","Spain","1504679459","abc","def"),
        Row(1,"4","India","1504679659","abc","def")
      )),
      StructType(expectedSchema)
    )

    assertDataFrameEquality(expectedDataFrame, transformationOutput("dataFrame3"), "DeviceId")
  }

  test("runSingleProcessorTransformationTest") {
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

    val transformations: List[Transformation[DataFrame]] = List(
      new TestProcessor
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    spark.conf.set("newColName", "newCol")
    val transformationOutput = spark.runTransformations(Map("dataFrame" -> dataFrame), transformations)

    val expectedSchema = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false),
      StructField("newCol2", StringType, false)
    )

    val expectedDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc","def"),
        Row(1,"2","India","1504679359","abc","def"),
        Row(1,"3","Spain","1504679459","abc","def"),
        Row(1,"4","India","1504679659","abc","def")
      )),
      StructType(expectedSchema)
    )

    assertDataFrameEquality(expectedDataFrame, transformationOutput("dataFrame3"), "DeviceId")
  }

  test("runMultipleProcessorTransformationsTest") {
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

    val transformations: List[Transformation[DataFrame]] = List(
      new TestProcessor,
      new TestProcessor2
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    spark.conf.set("newColName", "newCol")
    val transformationOutput = spark.runTransformations(Map("dataFrame" -> dataFrame), transformations)

    val expectedSchema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false),
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
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false),
      StructField("newCol2", StringType, false),
      StructField("random", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc","def",null),
        Row(1,"2","India","1504679359","abc","def",null),
        Row(1,"3","Spain","1504679459","abc","def",null),
        Row(1,"4","India","1504679659","abc","def",null)
      )),
      StructType(expectedSchema2)
    )

    assertDataFrameEquality(expectedDataFrame1, transformationOutput("dataFrame5"), "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, transformationOutput("dataFrame6"), "DeviceId")
  }

  test("runQueryAndProcessorTransformationsTest") {
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

    val transformations: List[Transformation[DataFrame]] = List(
      SparkSQLQueryProcessor("dataFrame2", "select *, 'abc' as newCol from dataFrame")(spark),
      SparkSQLQueryProcessor("dataFrame3", "select *, 'def' as newCol2 from dataFrame2")(spark),
      new TestProcessor2
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    val transformationOutput = spark.runTransformations(Map("dataFrame" -> dataFrame), transformations)

    val expectedSchema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false),
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
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, false),
      StructField("newCol2", StringType, false),
      StructField("random", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc","def",null),
        Row(1,"2","India","1504679359","abc","def",null),
        Row(1,"3","Spain","1504679459","abc","def",null),
        Row(1,"4","India","1504679659","abc","def",null)
      )),
      StructType(expectedSchema2)
    )

    assertDataFrameEquality(expectedDataFrame1, transformationOutput("dataFrame5"), "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, transformationOutput("dataFrame6"), "DeviceId")
  }

  test("writeToSinksTest") {
    val schema1 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val dataFrame1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559"),
        Row(1,"2","India","1504679359"),
        Row(1,"3","Spain","1504679459"),
        Row(1,"4","India","1504679659")
      )),
      StructType(schema1)
    )

    val schema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("DeviceId", StringType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true),
      StructField("newCol", StringType, true),
      StructField("newCol2", StringType, true)
    )

    val dataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1,"1","India","1504679559","abc","def"),
        Row(1,"2","India","1504679359","abc","def"),
        Row(1,"3","Spain","1504679459","abc","def"),
        Row(1,"4","India","1504679659","abc","def")
      )),
      StructType(schema2)
    )

    val sinks: List[Sink[DataFrame]] = List(
      SparkSink("dataFrame", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=09/dt=08/path1")),
      SparkSink("dataFrame2", SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=09/dt=08/path2"))
    )

    import com.zeotap.zeoflow.spark.constructs.SparkOps._
    spark.writeToSinks(Map("dataFrame" -> dataFrame1, "dataFrame2" -> dataFrame2), sinks)

    val actualDataFrame1 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=09/dt=08/path1")
    val actualDataFrame2 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2021/mon=09/dt=08/path2")

    assertDataFrameEquality(dataFrame1, actualDataFrame1, "DeviceId")
    assertDataFrameEquality(dataFrame2, actualDataFrame2, "DeviceId")
  }

}
