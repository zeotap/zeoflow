package com.zeotap.zeoflow.dsl

import com.zeotap.zeoflow.types.{Processor, QueryTransformation, Transformation, UDF}

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSources[A](sources: List[SourceBuilder]) extends FlowDSL[A]

  final case class LoadUserDefinedFunctions[A](udfs: List[UDF]) extends FlowDSL[A]

//  final case class RunSQLQueries[A](queries: List[QueryTransformation]) extends FlowDSL[A]
//
//  final case class RunUserDefinedProcessor[A](processor: Processor, inputTableNames: List[String], outputTableNames: List[String]) extends FlowDSL[A]

  final case class RunTransformations[A](transformations: List[Transformation]) extends FlowDSL[A]

  final case class WriteToSinks[A](sinks: List[SinkBuilder]) extends FlowDSL[A]

//  final case class AssertExpectation[A]() extends FlowDSL[A]
//
//  final case class SendAlert[A]() extends FlowDSL[A]

}
