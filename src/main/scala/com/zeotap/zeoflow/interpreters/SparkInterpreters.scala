package com.zeotap.zeoflow.interpreters

import cats.arrow.FunctionK
import cats.data.Reader
import com.zeotap.zeoflow.constructs.SparkOps.SparkExt
import com.zeotap.zeoflow.dsl.FlowDSL
import com.zeotap.zeoflow.dsl.FlowDSL._
import org.apache.spark.sql.SparkSession

object SparkInterpreters {

  type SparkFlow[A] = Reader[SparkSession, A]

  val sparkFlowInterpreter: FunctionK[FlowDSL, SparkFlow] = new FunctionK[FlowDSL, SparkFlow] {
    override def apply[A](feature: FlowDSL[A]): SparkFlow[A] = Reader { spark =>
      val flow: Unit = feature match {
        case LoadSources(sources) => spark.loadSources(sources)
        case LoadUserDefinedFunctions(udfs) => spark.loadUserDefinedFunctions(udfs)
        case RunTransformations(transformations) => spark.runTransformations(transformations)
        case WriteToSinks(sinks) => spark.writeToSinks(sinks)
      }
      flow.asInstanceOf[A]
    }
  }
}
