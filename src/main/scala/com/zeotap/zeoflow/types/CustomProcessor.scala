package com.zeotap.zeoflow.types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

class CustomProcessor(implicit spark: SparkSession) extends SparkProcessor {

  override def process(inputTableNames: List[String], outputTableNames: List[String]): Unit = {
    val df1 = spark.table(inputTableNames(0))
    val df2 = spark.table(inputTableNames(1))

    df1.withColumn("dummy", lit(null).cast("string")).createOrReplaceTempView(outputTableNames(0))
    df2.withColumn("random", lit(null).cast("string")).createOrReplaceTempView(outputTableNames(1))
  }

}
