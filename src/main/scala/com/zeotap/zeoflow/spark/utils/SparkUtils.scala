package com.zeotap.zeoflow.spark.utils

import com.zeotap.expectations.column.dsl.ColumnDSL
import org.apache.spark.sql.DataFrame

object SparkUtils {

  implicit class SparkContextExt(tables: Map[String, DataFrame]) {
    def moveTablesToContext: Map[String, DataFrame] = {
      tables.foreach(kv => kv._2.createOrReplaceTempView(kv._1))
      tables
    }

    def validateColumnDSL(columnDSL: Map[String, ColumnDSL]): Map[String, Boolean] = {

      val columnDSLDefinitionColumns: Map[String, Set[String]] = columnDSL.foldLeft(Map.empty: Map[String, Set[String]])((accMap, dsl) => {
        accMap ++ Map(dsl._1 -> dsl._2.columnExpectation.foldLeft(Set.empty: Set[String])((accList, cols) => {
          accList ++ Set(cols.name)
        }))
      })

      val inputTablesColumns: Map[String, Set[String]] = tables.foldLeft(Map.empty: Map[String, Set[String]])((accMap, input) => {
        accMap ++ Map(input._1 -> input._2.schema.fieldNames.toSet)
      })

      val fetchDslVerificationDetails: Map[String, Boolean] = inputTablesColumns.foldLeft(Map.empty: Map[String, Boolean])((accMap, input: (String, Set[String])) => {
        if(columnDSLDefinitionColumns.contains(input._1)) {
          if(columnDSLDefinitionColumns(input._1).subsetOf(input._2))
            accMap ++ Map(input._1 -> true)
          else
            throw new NoSuchElementException(String.format("Column DSL Columns should be subset of input dataframe columns. " +
              "ColumnDSL Column = %s | Input DataFrame Columns = %s", input._2, inputTablesColumns(input._1)))
        }
        else
          accMap ++ Map(input._1 -> false)
      })
      fetchDslVerificationDetails
    }
  }

}
