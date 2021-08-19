package com.zeotap.zeoflow.constructs

import cats.data.Reader
import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{ProcessorTransformation, QueryTransformation, Transformation, UDF}
import org.apache.spark.sql.SparkSession

object SparkOps {

  type SparkReader[A] = Reader[SparkSession, A]

  def loadSource(sources: List[SourceBuilder]): SparkReader[Unit] = Reader {
    spark => sources.foreach(source => source.build())
  }

  def loadUDFs(udfs: List[UDF]): SparkReader[Unit] = Reader {
    spark => udfs.foreach(udf => spark.udf.register(udf.name, udf.function))
  }

  def runTransformations(transformations: List[Transformation]): SparkReader[Unit] = Reader {
    spark => transformations.foreach {
      case qt: QueryTransformation => spark.sql(qt.query).createOrReplaceTempView(qt.name)
      case pt: ProcessorTransformation => pt.processor.process(pt.inputTableNames, pt.outputTableNames)
    }
  }

  def writeToSink(sinks: List[SinkBuilder]): SparkReader[Unit] = Reader {
    spark => sinks.foreach(sink => sink.build())
  }

  def preprocessProgram(sources: List[SourceBuilder],
                        udfs: List[UDF],
                        transformations: List[Transformation],
                        sinks: List[SinkBuilder]): SparkReader[Unit] = {
    for {
      _ <- loadSource(sources)
      _ <- loadUDFs(udfs)
      _ <- runTransformations(transformations)
      _ <- writeToSink(sinks)
    } yield sinks
  }

}
