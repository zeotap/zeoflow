package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.dsl.FlowDSLHelper.FreeFlowDSL
import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{ColumnAssertions, Transformation, UDF}
import com.zeotap.zeoflow.dsl.FlowDSLHelper._

object Production {

  def preprocessProgram[A](sources: List[SourceBuilder],
                        udfs: List[UDF],
                        transformations: List[Transformation],
                        sinks: List[SinkBuilder]): FreeFlowDSL[Unit] = {
    for {
      _ <- loadSources[A](sources)
      _ <- loadUserDefinedFunctions[A](udfs)
      _ <- runTransformations[A](transformations)
      _ <- writeToSinks[A](sinks)
    } yield sinks
  }

  def preprocessProgramWithAssertions[A](sources: List[SourceBuilder],
                                         udfs: List[UDF],
                                         transformations: List[Transformation],
                                         columnAssertions: List[ColumnAssertions],
                                         sinks: List[SinkBuilder]): FreeFlowDSL[Unit] = {

    for {
      _ <- loadSources[A](sources)
      _ <- loadUserDefinedFunctions[A](udfs)
      _ <- runTransformations[A](transformations)
      _ <- assertDataFrameExpectations[A](columnAssertions)
      _ <- writeToSinks[A](sinks)
    } yield sinks
  }

}
