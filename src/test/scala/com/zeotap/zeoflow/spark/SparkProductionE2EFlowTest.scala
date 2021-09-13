package com.zeotap.zeoflow.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.sink.spark.writer.SparkWriter
import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.common.constructs.Production
import com.zeotap.zeoflow.common.test.helpers.DataFrameUtils.assertDataFrameEquality
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

    val udfs: List[FlowUDF] = List(
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

}
