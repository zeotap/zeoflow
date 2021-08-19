package com.zeotap.zeoflow.dsl

import cats.free.Free
import cats.free.Free.liftF
import com.zeotap.zeoflow.dsl.FlowDSL.{RunSQLQueries, RunUserDefinedProcessor}
import com.zeotap.zeoflow.types.{Processor, QueryTransformation}

object FlowDSLHelper {

  type FreeFlowDSL[A] = Free[FlowDSL, A]

  def runSQLQueries[A](queries: List[QueryTransformation]): FreeFlowDSL[A] = liftF(RunSQLQueries(queries))

  def runUserDefinedProcessor[A](processor: Processor, inputTableNames: List[String], outputTableNames: List[String]): FreeFlowDSL[A] = liftF(RunUserDefinedProcessor(processor, inputTableNames, outputTableNames))

}
