package com.zeotap.zeoflow.common.dsl

import com.zeotap.zeoflow.common.types.{FlowUDF, SinkBuilder, SourceBuilder, Transformation}

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSources[A](sources: List[SourceBuilder[A]]) extends FlowDSL[A]

  final case class LoadUserDefinedFunctions[A](udfs: List[FlowUDF]) extends FlowDSL[A]

  final case class RunTransformations[A](transformations: List[Transformation[A]]) extends FlowDSL[A]

  final case class WriteToSinks[A](sinks: List[SinkBuilder[A]]) extends FlowDSL[A]

}
