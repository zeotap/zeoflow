package com.zeotap.zeoflow.spark.constructs

import com.zeotap.expectations.column.dsl.ColumnDSL
import com.zeotap.expectations.column.ops.ColumnExpectationOps.ColumnDSLOps
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import com.zeotap.zeoflow.common.types.{FlowUDF, Sink, Source, Transformation}
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

    def runColumnExpectation(inputTables: Map[String, DataFrame], columnDSL: Map[String, ColumnDSL]): Map[String, Map[String, ExpectationResult]] =

      columnDSL.foldLeft(Map.empty: Map[String, Map[String, ExpectationResult]])((accMap, columnDSL) => {
      if (inputTables.contains(columnDSL._1))
        accMap ++ Map(columnDSL._1 -> columnDSL._2.runOnSpark(inputTables(columnDSL._1)))
      else
        throw new NoSuchElementException("Production DataFrames do not have DP ColumnDSL Dataframes")

    })

  }

}
