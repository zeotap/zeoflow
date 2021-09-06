package com.zeotap.zeoflow.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.zeoflow.dsl.FlowDSL._
import com.zeotap.zeoflow.types._

object FlowDSLHelper {

  type FreeFlowDSL[A] = Free[FlowDSL, A]

  def loadSources[A](sources: List[SourceBuilder]): FreeFlowDSL[A] = liftF(LoadSources(sources))

  def loadUserDefinedFunctions[A](udfs: List[UDF]): FreeFlowDSL[A] = liftF(LoadUserDefinedFunctions(udfs))

  def runTransformations[A](transformations: List[Transformation]): FreeFlowDSL[A] = liftF(RunTransformations(transformations))

  def writeToSinks[A](sinks: List[SinkBuilder]): FreeFlowDSL[A] = liftF(WriteToSinks(sinks))

  def assertDataFrameExpectations[A](columnAssertions: List[ColumnAssertions]): FreeFlowDSL[A] = liftF(AssertExpectation(columnAssertions))

//  def runSQLQueries[A](queries: List[QueryTransformation]): FreeFlowDSL[A] = liftF(RunSQLQueries(queries))
//
//  def runUserDefinedProcessor[A](processor: Processor, inputTableNames: List[String], outputTableNames: List[String]): FreeFlowDSL[A] = liftF(RunUserDefinedProcessor(processor, inputTableNames, outputTableNames))

}
