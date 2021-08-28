package com.zeotap.zeoflow.dsl.source.spark

import com.zeotap.source.spark.loader.FSSparkLoader
import com.zeotap.zeoflow.dsl.source.Source
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkSource(sparkLoader: FSSparkLoader, tableName: String)(implicit spark: SparkSession) extends Source[DataFrame] {

  override def load(): DataFrame = sparkLoader.buildUnsafe

}
