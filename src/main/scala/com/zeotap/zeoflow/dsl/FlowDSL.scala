package com.zeotap.zeoflow.dsl

import com.zeotap.expectations.column.dsl.ColumnExpectation
import com.zeotap.zeoflow.types.{Processor, QueryTransformation}

sealed trait FlowDSL[A]

object FlowDSL {

  final case class LoadSource[A](sources: List[SourceBuilder]) extends FlowDSL[A]

  // udfs: List[String,typeOf[Function]
  final case class LoadUserDefinedFunction[A](whitelist: List[String],blacklist: List[String])
    extends FlowDSL[A]

  final case class RunSQLQueries[A](queries: List[QueryTransformation]) extends FlowDSL[A]

  final case class RunUserDefinedProcessor[A](processor: Processor, inputTableNames: List[String], outputTableNames: List[String]) extends FlowDSL[A]

  final case class WriteToSink[A](sinks: List[SinkBuilder]) extends FlowDSL[A]

  final case class AssertExpectation[A](sinkTables: List[String], columnExpectations: List[ColumnExpectation]) extends FlowDSL[A]

  final case class SendAlert[A]() extends FlowDSL[A]

}
