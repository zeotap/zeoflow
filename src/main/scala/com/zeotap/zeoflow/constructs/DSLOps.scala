package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.dsl.FlowDSLHelper.DSLF

object DSLOps {

  def featuresCompiler[A](features: Seq[DSLF[A]]): DSLF[A] =
    features.reduce((a, b) => for {
      _ <- a
      v2 <- b
    } yield v2)

}
