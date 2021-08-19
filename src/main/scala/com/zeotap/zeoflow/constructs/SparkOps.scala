package com.zeotap.zeoflow.constructs

import cats.data.Reader
import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{Processor, Query}
import org.apache.spark.sql.SparkSession

object SparkOps {

  type SparkReader[A] = Reader[SparkSession, A]

  def loadSource(sources: List[SourceBuilder]): SparkReader[Unit] = Reader {
    spark => sources.foreach(source => source.build())
  }

  def runSQLQueries(queries: List[Query]): SparkReader[Unit] = Reader {
    spark => queries.foreach(query => spark.sql(query.query).createOrReplaceTempView(query.name))
  }

  def runUserDefinedProcessor(processor: Processor, inputTableNames: List[String], outputTableNames: List[String]): SparkReader[Unit] = Reader {
    spark => processor.process(inputTableNames, outputTableNames)
  }

  def writeToSink(sinks: List[SinkBuilder]): SparkReader[Unit] = Reader {
    spark => sinks.foreach(sink => sink.build())
  }

  def preprocessProgram(sources: List[SourceBuilder],
                        queries: List[Query],
                        processor: Processor, inputTableNames: List[String], outputTableNames: List[String],
                        sinks: List[SinkBuilder]): SparkReader[Unit] = {
    for {
      _ <- loadSource(sources)
      _ <- runSQLQueries(queries)
      _ <- runUserDefinedProcessor(processor, inputTableNames, outputTableNames)
      _ <- writeToSink(sinks)
    } yield sinks
  }

}
