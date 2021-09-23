package com.zeotap.zeoflow.spark.constructs

import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.expectations.column.ops.ColumnExpectationOps.ColumnDSLOps
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}
import com.zeotap.zeoflow.spark.utils.SparkUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def loadSources(sources: List[Source[DataFrame]]): Map[String, DataFrame] = sources.map(source => source.load()).toMap

    def loadUserDefinedFunctions(udfs: List[FlowUDF[Unit]]): Unit = udfs.foreach(udf => udf.register())

    def runTransformations(inputTables: Map[String, DataFrame], transformations: List[Transformation[DataFrame]]): Map[String, DataFrame] =
      transformations.foldLeft(inputTables) { (accMap, transformation) =>
        accMap ++ transformation.postprocess(transformation.process(transformation.preprocess(accMap), spark.conf.getAll))
      }

    def writeToSinks(inputTables: Map[String, DataFrame], sinks: List[Sink[DataFrame]]): Unit = sinks.foreach(sink => sink.write(inputTables))

    def runColumnExpectation(inputTables: Map[String, DataFrame],
                             columnDSL: Map[String, Array[(ColumnExpectation, Boolean)]]): Map[String, (DataFrame, Map[String, ExpectationResult])] = {

      val getConsolidatedColumnDSL = columnDSL.foldLeft(Map.empty: Map[String, ColumnDSL])((accMap, dsl) => {
        accMap ++ Map(dsl._1 -> ColumnDSL(dsl._2.map(_._1): _*))
      })
      val columnDSLVerification = SparkContextExt(inputTables).validateColumnDSL(getConsolidatedColumnDSL)

      val columnExpectationResult = inputTables.foldLeft(Map.empty: Map[String, (DataFrame, Map[String, ExpectationResult])])((accMap, table) => {
        if (true.equals(columnDSLVerification(table._1)))
          accMap ++ Map(table._1 -> (inputTables(table._1), getConsolidatedColumnDSL(table._1).runOnSpark(inputTables(table._1))))
        else accMap ++ Map()
      })
      columnExpectationResult
    }
  }

}
