package com.zeotap.zeoflow.spark.test.processor

import com.zeotap.zeoflow.spark.types.SparkProcessor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class TestProcessor2 extends SparkProcessor {
  override def process(inputTables: Map[String, DataFrame], readOnlyGlobalCache: Map[String, Any]): Map[String, DataFrame] = {
    val df1 = inputTables("dataFrame2")
    val df2 = inputTables("dataFrame3")

    Map(
      "dataFrame5" -> df1.withColumn("dummy", lit(null).cast("string")),
      "dataFrame6" -> df2.withColumn("random", lit(null).cast("string"))
    )
  }
}
