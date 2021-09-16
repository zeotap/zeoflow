package com.zeotap.zeoflow.spark.types

import com.zeotap.zeoflow.common.types.FlowUDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

final case class SparkUDF(name: String, function: UserDefinedFunction)(implicit spark: SparkSession) extends FlowUDF[Unit] {
  override def register(): Unit = spark.udf.register(name, function)
}
