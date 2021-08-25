package com.zeotap.zeoflow.types

import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkSQLTransformation(query: String, name: String)(implicit spark:SparkSession) extends Transformation[DataFrame]{
  override def transform(): Map[String, DataFrame] = {
    val dataFrame = spark.sql(query)
    dataFrame.createOrReplaceTempView(name)
    Map(name->dataFrame)
  }
}
