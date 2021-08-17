package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.types.{Processor, Query}
import org.apache.spark.sql.SparkSession

object SparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def runSQLQueries(queries: List[Query]): Unit =
      queries.foreach(query => spark.sql(query.query).createOrReplaceTempView(query.name))

    def runUserDefinedProcessor(processor: Processor, inputTableNames: List[String], outputTableNames: List[String]): Unit =
      processor.process(inputTableNames, outputTableNames)

  }

}
