package com.zeotap.zeoflow.types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

sealed trait FlowUDF {
  def register(): Unit
}

final case class SparkUDF(name: String, function: UserDefinedFunction)(implicit spark: SparkSession) extends FlowUDF {
  override def register(): Unit = spark.udf.register(name, function)
}
