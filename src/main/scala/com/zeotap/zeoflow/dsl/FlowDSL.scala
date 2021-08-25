package com.zeotap.zeoflow.dsl

import com.zeotap.zeoflow.types.{Processor, SparkSQLTransformation}

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSource[A](sources: List[SourceBuilder]) extends FlowDSL[A]

  // udfs: List[String,typeOf[Function]
  final case class LoadUserDefinedFunction[A](whitelist: List[String],blacklist: List[String])
    extends FlowDSL[A]

  final case class RunSQLQueries[A](queries: List[SparkSQLTransformation]) extends FlowDSL[A]

  final case class RunUserDefinedProcessor[A](processor: Processor, inputTableNames: List[String], outputTableNames: List[String]) extends FlowDSL[A]

  final case class WriteToSink[A](sinks: List[SinkBuilder]) extends FlowDSL[A]

  final case class AssertExpectation[A]() extends FlowDSL[A]

  final case class SendAlert[A]() extends FlowDSL[A]

}
