package com.zeotap.zeoflow.spark.constructs

import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def loadSources(sources: List[Source[DataFrame]]): Map[String, DataFrame] = sources.map(source => source.load()).toMap

    def loadUserDefinedFunctions(udfs: List[FlowUDF[Unit]]): Unit = udfs.foreach(udf => udf.register())

    def runTransformations(inputTables: Map[String, DataFrame], transformations: List[Transformation[DataFrame]]): Map[String, DataFrame] =
      transformations.foldLeft(inputTables) { (accMap, transformation) =>
        accMap ++ transformation.postprocess(transformation.process(transformation.preprocess(accMap), spark.conf.getAll))
      }

    def writeToSinks(inputTables: Map[String, DataFrame], sinks: List[Sink[DataFrame]]): Unit = sinks.foreach(sink => sink.write(inputTables))

  }

}
