package com.zeotap.zeoflow.interpreters

import cats.arrow.FunctionK
import cats.data.Reader
import com.zeotap.zeoflow.constructs.SparkOps.SparkExt
import com.zeotap.zeoflow.dsl.FlowDSL
import com.zeotap.zeoflow.dsl.FlowDSL.RunSQLQueries
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkInterpreters {

  type SparkProcessor[A] = Reader[SparkSession, A]

  val sparkInterpreter: FunctionK[FlowDSL, SparkProcessor] = new FunctionK[FlowDSL, SparkProcessor] {
    override def apply[A](feature: FlowDSL[A]): SparkProcessor[A] = Reader { spark =>
      val dataFrames: List[DataFrame] = feature match {
        case RunSQLQueries(queries) => spark.runSQLQueries(queries)
      }
      dataFrames.asInstanceOf[A]
    }
  }

}
