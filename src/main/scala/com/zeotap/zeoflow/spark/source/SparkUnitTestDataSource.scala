package com.zeotap.zeoflow.spark.source

import com.zeotap.zeoflow.dsl.SourceBuilder
import org.apache.spark.sql.DataFrame

class SparkUnitTestDataSource(s: DataFrame) extends SourceBuilder[String]{
  override def build(): String = ???
}
