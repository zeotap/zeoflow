package com.zeotap.zeoflow.beam

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.data.io.helpers.beam.BeamTestHelpers
import com.zeotap.data.io.sink.beam.writer.BeamWriter
import com.zeotap.data.io.source.beam.loader.BeamLoader
import com.zeotap.zeoflow.beam.constructs.BeamContext
import com.zeotap.zeoflow.beam.interpreters.BeamInterpreters._
import com.zeotap.zeoflow.beam.types.{BeamSQLQueryProcessor, BeamSink, BeamSource, BeamUDF}
import com.zeotap.zeoflow.common.constructs.Production
import com.zeotap.zeoflow.common.test.helpers.DataFrameUtils.assertDataFrameEquality
import com.zeotap.zeoflow.common.types.Transformation
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.values.PCollection
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import java.io.File

class BeamProductionE2EFlowTest extends FunSuite with DataFrameSuiteBase with BeforeAndAfterEach {

  val schemaJson: String =
    """
      |{
      |  "type": "record",
      |  "name": "TEST_SCHEMA",
      |  "fields": [
      |    {
      |      "name": "Common_DataPartnerID",
      |      "type": "int",
      |      "default": 10
      |    },
      |    {
      |      "name": "DeviceId",
      |      "type": "string",
      |      "default": "null"
      |    },
      |    {
      |      "name": "Demographic_Country",
      |      "type": "string",
      |      "default": "null"
      |    },
      |    {
      |      "name": "Common_TS",
      |      "type": "string",
      |      "default": "null"
      |    }
      |  ]
      |}
      |""".stripMargin

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
        Row(1, "1", "India", "1504679559"),
        Row(1, "2", "India", "1504679359"),
        Row(1, "3", "Spain", "1504679459"),
        Row(1, "4", "India", "1504679659")
      )),
      StructType(schema)
    )

    dataFrame.write.format("avro").save("src/test/resources/custom-input-format/yr=2022/mon=01/dt=02")
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
    implicit val beam: Pipeline = Pipeline.create()

    val sources: List[BeamSource] = List(
      BeamSource("p1", BeamLoader.avro.schema(schemaJson).load("src/test/resources/custom-input-format/yr=2022/mon=01/dt=02"))
    )

    val transformations: List[Transformation[PCollection[org.apache.beam.sdk.values.Row]]] = List(
      BeamSQLQueryProcessor("p2", "select *, cast('abc' as varchar) as newCol from p1"),
      BeamSQLQueryProcessor("p3", "select DeviceId, newCol from p2"),
      BeamSQLQueryProcessor("p4", "select Common_DataPartnerID, Demographic_Country, Common_TS from p2")
    )

    val p3SchemaJson: String =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "DeviceId",
        |      "type": "string",
        |      "default": "null"
        |    },
        |    {
        |      "name": "newCol",
        |      "type": "string",
        |      "default": "null"
        |    }
        |  ]
        |}
        |""".stripMargin

    val p4SchemaJson: String =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "Common_DataPartnerID",
        |      "type": "int",
        |      "default": 10
        |    },
        |    {
        |      "name": "Demographic_Country",
        |      "type": "string",
        |      "default": "null"
        |    },
        |    {
        |      "name": "Common_TS",
        |      "type": "string",
        |      "default": "null"
        |    }
        |  ]
        |}
        |""".stripMargin

    val sinks: List[BeamSink] = List(
      BeamSink("p3", BeamWriter.avro.schema(p3SchemaJson).save("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path1")),
      BeamSink("p4", BeamWriter.avro.schema(p4SchemaJson).save("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path2"))
    )

    Production.e2eFlow(sources, List(), transformations, sinks).foldMap[BeamFlow](beamFlowInterpreter).run(BeamContext(Map(), List())).value._2
    beam.run()

    val expectedSchema1 = List(
      StructField("DeviceId", StringType, true),
      StructField("newCol", StringType, true)
    )

    val expectedDataFrame1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("1", "abc"),
        Row("2", "abc"),
        Row("3", "abc"),
        Row("4", "abc")
      )),
      StructType(expectedSchema1)
    )

    val expectedSchema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "India", "1504679559"),
        Row(1, "India", "1504679359"),
        Row(1, "Spain", "1504679459"),
        Row(1, "India", "1504679659")
      )),
      StructType(expectedSchema2)
    )

    val actualDataFrame1 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path1")
    val actualDataFrame2 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path2")

    assertDataFrameEquality(expectedDataFrame1, actualDataFrame1, "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, actualDataFrame2, "Common_TS")
  }

  test("e2eFlowTest") {
    implicit val beam: Pipeline = Pipeline.create()

    val sources: List[BeamSource] = List(
      BeamSource("p1", BeamLoader.avro.schema(schemaJson).load("src/test/resources/custom-input-format/yr=2022/mon=01/dt=02"))
    )

    val udfs: List[BeamUDF] = List(
      BeamUDF("cubic", BeamTestHelpers.getCubicIntegerFn)
    )

    val transformations: List[Transformation[PCollection[org.apache.beam.sdk.values.Row]]] = List(
      BeamSQLQueryProcessor("p2", "select *, cast('abc' as varchar) as newCol from p1"),
      BeamSQLQueryProcessor("p3", "select DeviceId, newCol from p2"),
      BeamSQLQueryProcessor("p4", "select Common_DataPartnerID, Demographic_Country, Common_TS from p2"),
      BeamSQLQueryProcessor("p5", "select cubic(DeviceId) as cubed, newCol from p2")
    )

    val p3SchemaJson: String =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "DeviceId",
        |      "type": "string",
        |      "default": "null"
        |    },
        |    {
        |      "name": "newCol",
        |      "type": "string",
        |      "default": "null"
        |    }
        |  ]
        |}
        |""".stripMargin

    val p4SchemaJson: String =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "Common_DataPartnerID",
        |      "type": "int",
        |      "default": 10
        |    },
        |    {
        |      "name": "Demographic_Country",
        |      "type": "string",
        |      "default": "null"
        |    },
        |    {
        |      "name": "Common_TS",
        |      "type": "string",
        |      "default": "null"
        |    }
        |  ]
        |}
        |""".stripMargin

    val p5SchemaJson: String =
      """
        |{
        |  "type": "record",
        |  "name": "TEST_SCHEMA",
        |  "fields": [
        |    {
        |      "name": "cubed",
        |      "type": "int",
        |      "default": 0
        |    },
        |    {
        |      "name": "newCol",
        |      "type": "string",
        |      "default": "null"
        |    }
        |  ]
        |}
        |""".stripMargin

    val sinks: List[BeamSink] = List(
      BeamSink("p3", BeamWriter.avro.schema(p3SchemaJson).save("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path1")),
      BeamSink("p4", BeamWriter.avro.schema(p4SchemaJson).save("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path2")),
      BeamSink("p5", BeamWriter.avro.schema(p5SchemaJson).save("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path3"))
    )

    Production.e2eFlow(sources, udfs, transformations, sinks).foldMap[BeamFlow](beamFlowInterpreter).run(BeamContext(Map(), List())).value._2
    beam.run()

    val expectedSchema1 = List(
      StructField("DeviceId", StringType, true),
      StructField("newCol", StringType, true)
    )

    val expectedDataFrame1 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("1", "abc"),
        Row("2", "abc"),
        Row("3", "abc"),
        Row("4", "abc")
      )),
      StructType(expectedSchema1)
    )

    val expectedSchema2 = List(
      StructField("Common_DataPartnerID", IntegerType, true),
      StructField("Demographic_Country", StringType, true),
      StructField("Common_TS", StringType, true)
    )

    val expectedDataFrame2 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "India", "1504679559"),
        Row(1, "India", "1504679359"),
        Row(1, "Spain", "1504679459"),
        Row(1, "India", "1504679659")
      )),
      StructType(expectedSchema2)
    )

    val expectedSchema3 = List(
      StructField("cubed", IntegerType, true),
      StructField("newCol", StringType, true)
    )

    val expectedDataFrame3 = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row(1, "abc"),
        Row(8, "abc"),
        Row(27, "abc"),
        Row(64, "abc")
      )),
      StructType(expectedSchema3)
    )

    val actualDataFrame1 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path1")
    val actualDataFrame2 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path2")
    val actualDataFrame3 = spark.read.format("avro").load("src/test/resources/custom-output-format/yr=2022/mon=01/dt=02/path3")

    assertDataFrameEquality(expectedDataFrame1, actualDataFrame1, "DeviceId")
    assertDataFrameEquality(expectedDataFrame2, actualDataFrame2, "Common_TS")
    assertDataFrameEquality(expectedDataFrame3, actualDataFrame3, "cubed")
  }

}
