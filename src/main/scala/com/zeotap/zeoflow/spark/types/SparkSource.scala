package com.zeotap.zeoflow.spark.types

import com.zeotap.source.spark.loader.SparkLoader
import com.zeotap.zeoflow.common.types.Source
import org.apache.spark.sql.{DataFrame, SparkSession}

final case class SparkSource(tableName: String, sparkLoader: SparkLoader)(implicit spark: SparkSession) extends Source[DataFrame] {
  override def load(): (String, DataFrame) = (tableName, sparkLoader.buildUnsafe)
}
