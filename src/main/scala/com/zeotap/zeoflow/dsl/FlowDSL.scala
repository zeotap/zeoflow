package com.zeotap.zeoflow.dsl

import com.zeotap.zeoflow.dsl.sink.Sink
import com.zeotap.zeoflow.dsl.source.Source
import com.zeotap.zeoflow.types.Transformation

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSource[A](sources: List[Source[A]]) extends FlowDSL[A]

  final case class LoadUserDefinedFunction[A](whitelist: List[String],blacklist: List[String])
    extends FlowDSL[A]

  // TODO: Implement a DAG here instead of a List
  final case class RunTransformations[A](queries: List[Transformation[A]]) extends FlowDSL[A]

  final case class WriteToSink[A](sinks: List[Sink]) extends FlowDSL[A]

  final case class AssertExpectation[A]() extends FlowDSL[A]

  final case class SendAlert[A]() extends FlowDSL[A]

}
