package com.zeotap.zeoflow.interpreters.beam

import cats.arrow.FunctionK
import cats.data.State
import com.zeotap.zeoflow.dsl.FlowDSL
import com.zeotap.zeoflow.dsl.FlowDSL.{LoadSource, RunTransformations, WriteToSink}
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple, Row}
import com.zeotap.zeoflow.interpreters.beam.BeamHelper._
import scala.collection.mutable


object BeamBasedInterpreters {
  //case class Beam(PCollection: PCollection[Row])
  type BeamStateStore[A] = State[mutable.Map[String, PCollection[Row]], A]

  new FunctionK[FlowDSL, BeamStateStore]{
    override def apply[A](fa: FlowDSL[A]): BeamStateStore[A] =State { state =>
      fa match {
        case LoadSource(sources) =>
        case RunTransformations(queries) => state.runSql(queries)
        case WriteToSink(sinks) =>
      }
    }
  }
}
