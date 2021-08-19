package com.zeotap.zeoflow.dsl

import com.zeotap.source.spark.loader.SparkLoader
import org.apache.spark.sql.SparkSession

trait SourceBuilder {

  def build(): Unit

}

case class SparkSourceBuilder(sparkLoader: SparkLoader, tableName: String)(implicit spark: SparkSession) extends SourceBuilder {

  override def build(): Unit = sparkLoader.buildSafe.right.get.createOrReplaceTempView(tableName)

}
