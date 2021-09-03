package com.zeotap.zeoflow

import java.io.File
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.sink.spark.writer.SparkWriter
import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.constructs.Production
import com.zeotap.zeoflow.interpreters.SparkInterpreters._
import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder, SparkSinkBuilder, SparkSourceBuilder}
import com.zeotap.zeoflow.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.zeoflow.test.processor.CustomProcessor2
import com.zeotap.zeoflow.types._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class ProductionE2EFlowTest extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(new File("src/test/resources/custom-input-format"))
    FileUtils.deleteQuietly(new File("src/test/resources/custom-output-format"))
  }

  test("e2eFlowWithoutUDFsTest") {
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

    implicit val sparkSession: SparkSession = spark

    val sources: List[SourceBuilder[DataFrame]] = List(
      SparkSourceBuilder(SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"), "dataFrame")
    )

    val transformations: List[Transformation[DataFrame]] = List(
      SparkQuery("select *, 'abc' as newCol from dataFrame", "dataFrame2"),
      SparkQuery("select DeviceId, newCol from dataFrame2", "dataFrame3"),
      SparkQuery("select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2", "dataFrame4"),
      new CustomProcessor2
    )

    val sinks: List[SinkBuilder[DataFrame]] = List(
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1"), "dataFrame5"),
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2"), "dataFrame6")
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

    implicit val sparkSession: SparkSession = spark

    val sources: List[SourceBuilder[DataFrame]] = List(
      SparkSourceBuilder(SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"), "dataFrame")
    )

    val dummyUDF: UserDefinedFunction = udf((column: String) => s"$column ABC")
    val sumUDF: UserDefinedFunction = udf((column1: String, column2: String) => s"$column1|$column2")

    val udfs: List[UDF] = List(
      UDF(dummyUDF, "dummyFunc"),
      UDF(sumUDF, "sumFunc")
    )

    val transformations: List[Transformation[DataFrame]] = List(
      SparkQuery("select *, 'abc' as newCol from dataFrame", "dataFrame2"),
      SparkQuery("select DeviceId, newCol from dataFrame2", "dataFrame3"),
      SparkQuery("select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2", "dataFrame4"),
      new CustomProcessor2,
      SparkQuery("select *, dummyFunc(newCol) as newCol2, sumFunc(newCol, random) as newCol3 from dataFrame6", "dataFrame7")
    )

    val sinks: List[SinkBuilder[DataFrame]] = List(
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1"), "dataFrame5"),
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2"), "dataFrame6"),
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path3"), "dataFrame7")
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
