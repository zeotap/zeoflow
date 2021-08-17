package com.zeotap.zeoflow.interpreters

import cats.arrow.FunctionK
import cats.data.{Reader, State}
import com.zeotap.zeoflow.constructs.SparkOps.SparkExt
import com.zeotap.zeoflow.dsl.FlowDSL
import com.zeotap.zeoflow.dsl.FlowDSL.{RunSQLQueries, RunUserDefinedProcessor}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkInterpreters {

  type SparkProcessor[A] = Reader[SparkSession, A]

  type SparkDataFrames[A] = State[List[DataFrame], A]

  val sparkInterpreter: FunctionK[FlowDSL, SparkProcessor] = new FunctionK[FlowDSL, SparkProcessor] {
    override def apply[A](feature: FlowDSL[A]): SparkProcessor[A] = Reader { spark =>
      val units: Unit = feature match {
        case RunSQLQueries(queries) => spark.runSQLQueries(queries)
        case RunUserDefinedProcessor(processor, inputTableNames, outputTableNames) => spark.runUserDefinedProcessor(processor, inputTableNames, outputTableNames)
        case _ => println("error")
      }
      units.asInstanceOf[A]
    }
  }

}
