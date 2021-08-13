package com.zeotap.zeoflow.dsl

import com.zeotap.zeoflow.types.Query

sealed trait FlowDSL[A]

object FlowDSL {

  final case class RunSQLQueries[A](queries: List[Query]) extends FlowDSL[A]

}
