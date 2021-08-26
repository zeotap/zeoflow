package com.zeotap.zeoflow.constructs

import cats.data.Reader
import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.expectations.column.ops.ColumnExpectationOps.ColumnDSLOps
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{ProcessorTransformation, QueryTransformation, Transformation, UDF}
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

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

  def runColumnExpectationOnSink(sinkTables: List[String], columnExpectations: List[ColumnExpectation]): SparkReader[List[Map[String, ExpectationResult]]] =  Reader {
//    val x = List(ColumnDSL(columnExpectation.head).runOnSpark(spark.table(sinkTables.head)))
    spark => columnExpectations.map(colExp => ColumnDSL(colExp).runOnSpark(spark.table(sinkTables.head)))
  }

  def writeToSink(sinks: List[SinkBuilder]): SparkReader[Unit] = Reader {
    spark => sinks.foreach(sink => sink.build())
  }

  def preprocessProgram(sources: List[SourceBuilder],
                        udfs: List[UDF],
                        transformations: List[Transformation],
                        sinks: List[SinkBuilder],
                        sinkTables: List[String],
                        columnExpectations: List[ColumnExpectation]): SparkReader[Unit] = {
    for {
      _ <- loadSource(sources)
      _ <- loadUDFs(udfs)
      _ <- runTransformations(transformations)
      _ <- runColumnExpectationOnSink(sinkTables, columnExpectations)
      _ <- writeToSink(sinks)
    } yield sinks
  }

}
