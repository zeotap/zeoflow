package com.zeotap.zeoflow.constructs

import cats.data.Reader
import org.apache.spark.sql.SparkSession

object SparkOps {

  type SparkReader[A] = Reader[SparkSession, A]

//  def loadSource(sources: List[Source]): SparkReader[Unit] = Reader {
//    spark => sources.foreach(source => source.load())
//  }
//
//  def loadUDFs(udfs: List[UDF]): SparkReader[Unit] = Reader {
//    spark => udfs.foreach(udf => spark.udf.register(udf.name, udf.function))
//  }
//
//  def runTransformations[A](transformations: List[Transformation[A]]): SparkReader[Unit] = Reader {
//    spark => transformations.foreach {
//      case qt: SparkSQLTransformation => spark.sql(qt.query).createOrReplaceTempView(qt.name)
//      case pt: SparkProcessorTransformation => pt.processor.process(pt.inputTables, Map())
//    }
//  }
//
//  def writeToSink(sinks: List[Sink]): SparkReader[Unit] = Reader {
//    spark => sinks.foreach(sink => sink.write())
//  }



}
