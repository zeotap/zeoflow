package com.zeotap.zeoflow.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.zeoflow.dsl.FlowDSL._
import com.zeotap.zeoflow.types._

object FlowDSLHelper {

  type FreeFlowDSL[A] = Free[FlowDSL, A]

  def loadSources[A](sources: List[SourceBuilder[A]]): FreeFlowDSL[A] = liftF(LoadSources(sources))

  def loadUserDefinedFunctions[A](udfs: List[UDF]): FreeFlowDSL[A] = liftF(LoadUserDefinedFunctions(udfs))

  def runTransformations[A](transformations: List[Transformation[A]]): FreeFlowDSL[A] = liftF(RunTransformations(transformations))

  def writeToSinks[A](sinks: List[SinkBuilder[A]]): FreeFlowDSL[A] = liftF(WriteToSinks(sinks))

}
