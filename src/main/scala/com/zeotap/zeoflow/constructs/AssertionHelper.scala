package com.zeotap.zeoflow.constructs

import com.zeotap.expectations.column.dsl.ColumnDSL
import com.zeotap.expectations.column.ops.ColumnExpectationOps.ColumnDSLOps
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.utility.spark.ops.SparkDataframeOps.SparkOps
import com.zeotap.utility.spark.types.SparkDataframe
import com.zeotap.zeoflow.DpExpectation.DpObjectFetcher
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalactic.anyvals.PosInt
import org.scalatest.prop.GeneratorDrivenPropertyChecks

object AssertionHelper extends GeneratorDrivenPropertyChecks {

  def prodAssertion(dpName: String, region: String, productType: String, inputTables: List[String]): List[(String, Map[String, ExpectationResult])] = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val dpObj = new DpObjectFetcher().dataPartnerAssertions(dpName)
    val dpColumnDSL: List[(String, ColumnDSL)]  = dpObj.getColumnDSL(region, productType).toList

    var columnExpectations: List[(String, Map[String, ExpectationResult])] = List()

    dpColumnDSL.foreach(colDsl => {
      if (inputTables.contains(colDsl._1)) {
        columnExpectations = columnExpectations :+ ((colDsl._1, colDsl._2.runOnSpark(spark.table(colDsl._1))))
      }
      else { throw new NoSuchElementException("Production Input table Names does not match with DP ColumnDSL Dataframes")}
    })

    println(columnExpectations)

    columnExpectations

  }

  def qaAssertions(dpName: String, region: String, productType: String, minDfSize: PosInt,
                   totalDataframes: PosInt): List[(String, List[Map[String, ExpectationResult]])] = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val dpObj = new DpObjectFetcher().dataPartnerAssertions(dpName)
    val dpColumnDSL = dpObj.getColumnDSL(region, productType)
    val dpDataframeDefinitions = dpObj.getRawDataDefinition(region, productType)

    val unitTestColumnDslAndDF: List[((String, ColumnDSL), (String, SparkDataframe))] = dpColumnDSL.zip(dpDataframeDefinitions).toList

    var columnDslWithSparkUtilDF: List[(String, List[(DataFrame, ColumnDSL)])] = List()

    unitTestColumnDslAndDF.foreach(ut => {
      if (ut._1._1.equals(ut._2._1)) {
        var dataframeList: List[(DataFrame, ColumnDSL)] = List()
        forAll(ut._2._2.getArbitraryGenerator()(spark), configParams = minSize(minDfSize), minSuccessful(totalDataframes)) {
          df => dataframeList = dataframeList :+ (df, ut._1._2)
        }
        columnDslWithSparkUtilDF = columnDslWithSparkUtilDF :+ (ut._1._1, dataframeList)
      }
      else {
        throw new NotImplementedError("ColumnDSL and RawInput DataFrame Definition need to be in same order")
      }
    })

    var qaColumnExpectations: List[(String, List[Map[String, ExpectationResult]])] = List()

    columnDslWithSparkUtilDF.foreach(colDsl => {
      var columnExp: List[Map[String, ExpectationResult]] = List()
      colDsl._2.foreach(dfDsl => {
        columnExp = columnExp :+ dfDsl._2.runOnSpark(dfDsl._1)
      })
      qaColumnExpectations = qaColumnExpectations :+ (colDsl._1, columnExp)
    })

    println(qaColumnExpectations)

    qaColumnExpectations
  }

}
