package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.types.Query
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def runSQLQueries(queries: List[Query]): List[DataFrame] = {
      queries.foldLeft(List().asInstanceOf[List[DataFrame]])((newList, query) => {
        val dataFrame = spark.sql(query.query)
        dataFrame.createOrReplaceTempView(query.name)

        newList :+ dataFrame
      })
    }

  }

}
