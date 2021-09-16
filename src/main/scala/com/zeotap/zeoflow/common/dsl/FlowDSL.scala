package com.zeotap.zeoflow.common.dsl

import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSources[A](sources: List[Source[A]]) extends FlowDSL[A]

  final case class LoadUserDefinedFunctions[A](udfs: List[FlowUDF[A]]) extends FlowDSL[A]

  final case class RunTransformations[A](transformations: List[Transformation[A]]) extends FlowDSL[A]

  final case class WriteToSinks[A](sinks: List[Sink[A]]) extends FlowDSL[A]

}
