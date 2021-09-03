package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{ProcessorTransformation, QueryTransformation, Transformation, UDF}
import org.apache.spark.sql.SparkSession

object SparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def loadSources(sources: List[SourceBuilder]): Unit = sources.foreach(source => source.build())

    def loadUserDefinedFunctions(udfs: List[UDF]): Unit = udfs.foreach(udf => spark.udf.register(udf.name, udf.function))

    def runTransformations(transformations: List[Transformation]): Unit =
      transformations.foreach {
        case qt: QueryTransformation => spark.sql(qt.query).createOrReplaceTempView(qt.name)
        case pt: ProcessorTransformation => pt.processor.process(pt.inputTableNames, pt.outputTableNames)
      }

    def writeToSinks(sinks: List[SinkBuilder]): Unit = sinks.foreach(sink => sink.build())

  }

}
