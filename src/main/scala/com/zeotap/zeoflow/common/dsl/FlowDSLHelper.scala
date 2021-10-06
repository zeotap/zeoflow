package com.zeotap.zeoflow.common.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.zeoflow.common.dsl.FlowDSL._
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}

object FlowDSLHelper {

  type FreeFlowDSL[A] = Free[FlowDSL, A]

  def loadSources[A](sources: List[Source[A]]): FreeFlowDSL[A] = liftF(LoadSources(sources))

  def loadUserDefinedFunctions[A](udfs: List[FlowUDF[A]]): FreeFlowDSL[A] = liftF(LoadUserDefinedFunctions(udfs))

  def runTransformations[A](transformations: List[Transformation[A]]): FreeFlowDSL[A] = liftF(RunTransformations(transformations))

  def writeToSinks[A](sinks: List[Sink[A]]): FreeFlowDSL[A] = liftF(WriteToSinks(sinks))

  def runColumnExpectation[A](columnDSL: Map[String, ColumnDSL]): FreeFlowDSL[A] = liftF(FetchColumnExpectation(columnDSL))

}
