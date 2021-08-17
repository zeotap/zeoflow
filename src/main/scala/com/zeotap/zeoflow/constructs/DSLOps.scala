package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.dsl.FlowDSLHelper.FreeFlowDSL

object DSLOps {

  def featuresCompiler[A](features: Seq[FreeFlowDSL[A]]): FreeFlowDSL[A] =
    features.reduce((a, b) => for {
      _ <- a
      v2 <- b
    } yield v2)

}
