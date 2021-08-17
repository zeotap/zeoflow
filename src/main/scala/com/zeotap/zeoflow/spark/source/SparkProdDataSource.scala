package com.zeotap.zeoflow.spark.source

import com.zeotap.source.spark.loader.FSSparkLoader
import com.zeotap.zeoflow.dsl.SourceBuilder
import org.apache.spark.sql.DataFrame

case class SparkProdDataSource(s: FSSparkLoader) extends SourceBuilder[DataFrame]
