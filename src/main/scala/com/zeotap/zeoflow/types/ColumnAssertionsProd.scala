package com.zeotap.zeoflow.types

case class ColumnAssertionsProd(dpName: String, region: String,
                                productType: String, inputTableNames: List[String]) extends ColumnAssertions
