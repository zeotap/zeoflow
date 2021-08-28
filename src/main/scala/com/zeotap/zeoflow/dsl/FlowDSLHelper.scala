package com.zeotap.zeoflow.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.zeoflow.dsl.FlowDSL.{LoadSource, LoadUserDefinedFunction, RunTransformations, WriteToSink}
import com.zeotap.zeoflow.dsl.sink.Sink
import com.zeotap.zeoflow.dsl.source.Source
import com.zeotap.zeoflow.types.Transformation

object FlowDSLHelper {

  type FreeFlowDSL[A] = Free[FlowDSL, A]

  def runTransformations[A](queries: List[Transformation[A]]): FreeFlowDSL[A] = liftF(RunTransformations(queries))

  def loadSources[A](sources: List[Source[A]]): FreeFlowDSL[A] = liftF(LoadSource(sources))

  def writeToSink[A](sinks: List[Sink]): FreeFlowDSL[A] = liftF(WriteToSink(sinks))

  def loadUserDefinedFunctions[A](whitelist: List[String], blacklist: List[String]): FreeFlowDSL[A] =
    liftF(LoadUserDefinedFunction(whitelist, blacklist))

}
