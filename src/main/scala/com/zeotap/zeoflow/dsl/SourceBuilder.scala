package com.zeotap.zeoflow.dsl

import com.zeotap.source.spark.loader.SparkLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

trait SourceBuilder[A] {
  def build(): Map[String, A]
}

case class SparkSourceBuilder(sparkLoader: SparkLoader, tableName: String)(implicit spark: SparkSession) extends SourceBuilder[DataFrame] {
  override def build(): Map[String, DataFrame] = Map(tableName -> sparkLoader.buildUnsafe)
}
