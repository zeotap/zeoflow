package com.zeotap.zeoflow.dsl

import com.zeotap.source.spark.loader.SparkLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait SourceBuilder[A] {
  def build(): (String, A)
}

final case class SparkSourceBuilder(tableName: String, sparkLoader: SparkLoader)(implicit spark: SparkSession) extends SourceBuilder[DataFrame] {
  override def build(): (String, DataFrame) = (tableName, sparkLoader.buildUnsafe)
}
