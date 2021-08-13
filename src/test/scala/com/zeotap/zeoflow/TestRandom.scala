package com.zeotap.zeoflow

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.zeoflow.constructs.DSLOps.featuresCompiler
import com.zeotap.zeoflow.dsl.FlowDSLHelper
import com.zeotap.zeoflow.dsl.FlowDSLHelper.DSLF
import com.zeotap.zeoflow.interpreters.SparkInterpreters.{SparkProcessor, sparkInterpreter}
import com.zeotap.zeoflow.types.Query
import org.apache.spark.sql.{DataFrame, Row}
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

    dataFrame.createOrReplaceTempView("dataFrame")

    dataFrame.printSchema()
    dataFrame.show(false)

    val queries: List[Query] = List(
      Query("select *, 'abc' as new_col from dataFrame", "dataFrame2"),
      Query("select DeviceId, new_col from dataFrame2", "dataFrame3"),
      Query("select Common_DataPartnerID, Demographic_Country, Common_TS from dataFrame2", "dataFrame4")
    )

    val dslSeq: Seq[DSLF[List[DataFrame]]] = Seq(FlowDSLHelper.runSQLQueries(queries))

    val dataFrames = featuresCompiler(dslSeq).foldMap[SparkProcessor](sparkInterpreter).run(spark)
    dataFrames.foreach(df => {
      df.printSchema()
      df.show(false)
    })
  }

}
