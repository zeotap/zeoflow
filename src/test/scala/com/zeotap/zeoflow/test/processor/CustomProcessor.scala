package com.zeotap.zeoflow.test.processor

import com.zeotap.zeoflow.types.SparkProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class CustomProcessor extends SparkProcessor {
  override def process(inputTables: Map[String, DataFrame], cache: Map[String, Any]): Map[String, DataFrame] = {
    val df = inputTables("dataFrame")
    val df2 = df.withColumn("newCol", lit("abc").cast("string"))

    Map(
      "dataFrame2" -> df2,
      "dataFrame3" -> df2.withColumn("newCol2", lit("def").cast("string"))
    )
  }
}
