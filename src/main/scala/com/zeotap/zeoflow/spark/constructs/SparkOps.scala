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

    def runColumnExpectation(inputTables: Map[String, DataFrame], columnDSL: Map[String, ColumnDSL]): Map[String, Map[String, ExpectationResult]] = {
      require(columnDSL.keySet.subsetOf(inputTables.keySet), String.format("Input tables must contain column DSL tables. " +
        "Difference = %s", columnDSL.keySet.diff(inputTables.keySet)))

      columnDSL.foldLeft(Map.empty: Map[String, Map[String, ExpectationResult]])((accMap, columnDSL) =>
        accMap ++ Map(columnDSL._1 -> columnDSL._2.runOnSpark(inputTables(columnDSL._1))))

    }
  }

}
