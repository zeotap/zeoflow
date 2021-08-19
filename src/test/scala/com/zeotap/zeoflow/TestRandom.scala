package com.zeotap.zeoflow

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.sink.spark.writer.SparkWriter
import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.constructs.SparkOps
import com.zeotap.zeoflow.dsl.{SourceBuilder, SparkSinkBuilder, SparkSourceBuilder}
import com.zeotap.zeoflow.types.{CustomProcessor, Query}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

class TestRandom extends FunSuite with DataFrameSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
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

    val sources: List[SourceBuilder] = List(
      SparkSourceBuilder(SparkLoader.avro.load("src/test/resources/custom-input-format/yr=2021/mon=08/dt=19"), "dataFrame")(spark)
    )

    val queries: List[Query] = List(
      Query("select *, 'abc' as new_col from dataFrame", "dataFrame2"),
      Query("select DeviceId, new_col from dataFrame2", "dataFrame3"),
      Query("select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2", "dataFrame4")
    )

    val processor = new CustomProcessor()(spark)
    val inputTableNames = List("dataFrame2", "dataFrame3")
    val outputTableNames = List("dataFrame5", "dataFrame6")

    val sinks = List(
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1"), "dataFrame5")(spark),
      SparkSinkBuilder(SparkWriter.avro.save("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2"), "dataFrame6")(spark)
    )

    SparkOps.preprocessProgram(sources, queries, processor, inputTableNames, outputTableNames, sinks).run(spark)

    List("src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path1", "src/test/resources/custom-output-format/yr=2021/mon=08/dt=19/path2").foreach(path => {
      val df = spark.read.format("avro").load(path)
      df.printSchema()
      df.show(false)
    })
  }

}
