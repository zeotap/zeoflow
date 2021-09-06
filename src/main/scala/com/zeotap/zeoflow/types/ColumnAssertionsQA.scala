package com.zeotap.zeoflow.types

import org.scalactic.anyvals.PosInt

case class ColumnAssertionsQA(dpName: String, region: String, productType: String,
                              minDataframeSize: PosInt, totalDFsRequired: PosInt) extends ColumnAssertions