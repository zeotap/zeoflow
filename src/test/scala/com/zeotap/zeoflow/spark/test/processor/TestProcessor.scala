package com.zeotap.zeoflow.spark.test.processor

import com.zeotap.zeoflow.spark.types.SparkProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class TestProcessor extends SparkProcessor {
  override def process(inputTables: Map[String, DataFrame], readOnlyGlobalCache: Map[String, Any]): Map[String, DataFrame] = {
    val newColName = readOnlyGlobalCache("newColName").toString

    val df = inputTables("dataFrame")
    val df2 = df.withColumn(newColName, lit("abc").cast("string"))

    Map(
      "dataFrame2" -> df2,
      "dataFrame3" -> df2.withColumn("newCol2", lit("def").cast("string"))
    )
  }
}
