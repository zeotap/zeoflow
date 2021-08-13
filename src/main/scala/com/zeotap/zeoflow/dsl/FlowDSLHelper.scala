package com.zeotap.zeoflow.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.zeoflow.dsl.FlowDSL.RunSQLQueries
import com.zeotap.zeoflow.types.Query

object FlowDSLHelper {

  type DSLF[A] = Free[FlowDSL, A]

  def runSQLQueries[A](queries: List[Query]): DSLF[A] = liftF(RunSQLQueries(queries))

}
