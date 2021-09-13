package com.zeotap.zeoflow.common.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.zeoflow.common.dsl.FlowDSL._
import com.zeotap.zeoflow.common.types.{FlowUDF, SinkBuilder, SourceBuilder, Transformation}

object FlowDSLHelper {

  type FreeFlowDSL[A] = Free[FlowDSL, A]

  def loadSources[A](sources: List[SourceBuilder[A]]): FreeFlowDSL[A] = liftF(LoadSources(sources))

  def loadUserDefinedFunctions[A](udfs: List[FlowUDF]): FreeFlowDSL[A] = liftF(LoadUserDefinedFunctions(udfs))

  def runTransformations[A](transformations: List[Transformation[A]]): FreeFlowDSL[A] = liftF(RunTransformations(transformations))

  def writeToSinks[A](sinks: List[SinkBuilder[A]]): FreeFlowDSL[A] = liftF(WriteToSinks(sinks))

}
