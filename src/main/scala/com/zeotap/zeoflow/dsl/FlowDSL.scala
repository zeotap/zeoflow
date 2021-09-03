package com.zeotap.zeoflow.dsl

import com.zeotap.zeoflow.types.{Transformation, UDF}

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSources[A](sources: List[SourceBuilder[A]]) extends FlowDSL[A]

  final case class LoadUserDefinedFunctions[A](udfs: List[UDF]) extends FlowDSL[A]

  final case class RunTransformations[A](transformations: List[Transformation[A]]) extends FlowDSL[A]

  final case class WriteToSinks[A](sinks: List[SinkBuilder[A]]) extends FlowDSL[A]

//  final case class AssertExpectation[A]() extends FlowDSL[A]
//
//  final case class SendAlert[A]() extends FlowDSL[A]

}
