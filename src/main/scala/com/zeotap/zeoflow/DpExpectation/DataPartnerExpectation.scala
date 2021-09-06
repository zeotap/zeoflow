package com.zeotap.zeoflow.DpExpectation

import com.zeotap.expectations.column.dsl.ColumnDSL
import com.zeotap.utility.spark.types.SparkDataframe

trait DataPartnerExpectation {

  def getColumnDSL(region: String, product: String): Map[String, ColumnDSL]

  def getRawDataDefinition(region: String, product: String): Map[String, SparkDataframe]

}
