package com.zeotap.zeoflow

import java.io.File
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.sink.spark.writer.SparkWriter
import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.constructs.SparkOps
import com.zeotap.zeoflow.dsl.source.{Source, SparkSourceBuilder}
import com.zeotap.zeoflow.dsl.SparkSourceBuilder
import com.zeotap.zeoflow.dsl.sink.SparkSinkBuilder
import com.zeotap.zeoflow.types._
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class TestRandom extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.deleteQuietly(new File("src/test/resources/custom-input-format"))
    FileUtils.deleteQuietly(new File("src/test/resources/custom-output-format"))
  }

  test("queries") {
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

    val sources: List[Source] = List(
      SparkSourceBuilder(SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"), "dataFrame")(spark)
    )

    val dummyUDF: UserDefinedFunction = udf((column: String) => s"$column ABC")
    val sumUDF: UserDefinedFunction = udf((column1: String, column2: String) => s"$column1|$column2")

    val udfs: List[UDF] = List(
      UDF(dummyUDF, "dummyFunc"),
      UDF(sumUDF, "sumFunc")
    )

    val transformations: List[Transformation] = List(
      SparkSQLTransformation("select *, 'abc' as newCol from dataFrame", "dataFrame2"),
      SparkSQLTransformation("select DeviceId, newCol from dataFrame2", "dataFrame3"),
      SparkSQLTransformation("select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2", "dataFrame4"),
      ProcessorTransformation(new CustomProcessor()(spark), List("dataFrame2", "dataFrame3"), List("dataFrame5", "dataFrame6")),
      SparkSQLTransformation("select *, dummyFunc(newCol) as newCol2, sumFunc(newCol, random) as newCol3 from dataFrame6", "dataFrame7")
    )

    val sinks = List(
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1"), "dataFrame5")(spark),
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2"), "dataFrame6")(spark),
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path3"), "dataFrame7")(spark)
    )

    SparkOps.preprocessProgram(sources, udfs, transformations, sinks).run(spark)

    List(
      "src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1",
      "src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2",
      "src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path3"
    ).foreach(path => {
      val df = spark.read.format("avro").load(path)
      df.printSchema()
      df.show(false)
    })
  }

}
