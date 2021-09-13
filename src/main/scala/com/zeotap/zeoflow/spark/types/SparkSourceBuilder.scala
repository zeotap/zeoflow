package com.zeotap.zeoflow.spark.types

import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.common.types.SourceBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class SparkSourceBuilder(tableName: String, sparkLoader: SparkLoader)(implicit spark: SparkSession) extends SourceBuilder[DataFrame] {
  override def build(): (String, DataFrame) = (tableName, sparkLoader.buildUnsafe)
}
