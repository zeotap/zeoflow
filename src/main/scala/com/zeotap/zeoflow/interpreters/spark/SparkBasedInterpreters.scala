package com.zeotap.zeoflow.interpreters.spark

import cats.arrow.FunctionK
import cats.data.State
import com.zeotap.zeoflow.dsl.FlowDSL
import com.zeotap.zeoflow.dsl.FlowDSL.LoadSource
import org.apache.spark.sql.DataFrame

object SparkBasedInterpreters {
  type SparkStateStore[A] = State[Map[String,DataFrame], A]
    val flowInterpreter= new FunctionK[FlowDSL, SparkStateStore] {
      override def apply[A](fa: FlowDSL[A]): SparkStateStore[A] = fa match {
        case LoadSource(sources) =>sources
      }{


      }
    }
}
