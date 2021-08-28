package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.constructs.SparkOps.{SparkReader, loadSource, loadUDFs, runTransformations, writeToSink}
import com.zeotap.zeoflow.dsl.FlowDSLHelper.{FreeFlowDSL, loadSources, loadUserDefinedFunctions, runTransformations, writeToSink}
import com.zeotap.zeoflow.dsl.sink.Sink
import com.zeotap.zeoflow.dsl.source.Source
import com.zeotap.zeoflow.types.{Transformation, UDF}

object Production {
  def compileWithoutUDF[A](sources: List[Source[A]],
                           transformations: List[Transformation[A]],
                           sinks: List[Sink]): FreeFlowDSL[Unit] = {
    for {
      _ <- loadSources(sources)
      t <- runTransformations(transformations)
      _ <- writeToSink(sinks)
    } yield ()
  }


  def compileWithUDF[A](sources: List[Source[A]],
                           udfs: List[UDF],
                           transformations: List[Transformation[A]],
                           sinks: List[Sink]): FreeFlowDSL[Unit] = {
    for {
      _ <- loadSources(sources)
      //_ <- loadUserDefinedFunctions()
      t <- runTransformations(transformations)
      _ <- writeToSink(sinks)
    } yield ()
  }
}
