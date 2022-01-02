package com.zeotap.zeoflow.common.constructs

import com.zeotap.expectations.column.dsl.ColumnDSL
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.zeoflow.common.dsl.FlowDSLHelper._
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}

object Production {

  def e2eFlow[A, B](sources: List[Source[A]],
                 udfs: List[FlowUDF[B]] = List(),
                 transformations: List[Transformation[A]],
                 sinks: List[Sink[A]]): FreeFlowDSL[Unit] = {
    for {
      _ <- loadSources[A](sources)
      _ <- loadUserDefinedFunctions[B](udfs)
      _ <- runTransformations[A](transformations)
      _ <- writeToSinks[A](sinks)
    } yield Unit
  }

  def e2eFlowWithExpectations[A, B](sources: List[Source[A]],
                                  udfs: List[FlowUDF[B]] = List(),
                                  transformations: List[Transformation[A]],
                                  columnDSL: Map[String, ColumnDSL]): FreeFlowDSL[Map[String, Map[String, ExpectationResult]]] = {

    for {
      _ <- loadSources[A](sources)
      _ <- loadUserDefinedFunctions[B](udfs)
      _ <- runTransformations[A](transformations)
      expectationResult <- assertExpectation[Map[String, Map[String, ExpectationResult]]](columnDSL)
    } yield expectationResult
  }

}
