package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{Transformation, UDF}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def loadSources(sources: List[SourceBuilder[DataFrame]]): Map[String, DataFrame] = sources.flatMap(source => source.build()).toMap

    def loadUserDefinedFunctions(udfs: List[UDF]): Unit = udfs.foreach(udf => spark.udf.register(udf.name, udf.function))

    def runTransformations(inputTables: Map[String, DataFrame], transformations: List[Transformation[DataFrame]]): Map[String, DataFrame] = {
      transformations.foldLeft(inputTables){(accMap, transformation) =>
        accMap ++ transformation.postprocess(transformation.process(transformation.preprocess(accMap), spark.conf.getAll))
      }
    }

    def writeToSinks(inputTables: Map[String, DataFrame], sinks: List[SinkBuilder[DataFrame]]): Unit = sinks.foreach(sink => sink.build(inputTables))

  }

}
