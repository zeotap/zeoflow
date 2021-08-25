package com.zeotap.zeoflow.types

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class CustomProcessor/*(implicit spark: SparkSession)*/ extends Processor[DataFrame] {

  override def process(inputTableNames: Map[String, DataFrame],names:List[String]): Map[String, DataFrame] = {
    val df1 = inputTableNames("name1")//spark.table(inputTableNames(0))
    val df2 = inputTableNames("name2")//spark.table(inputTableNames(1))

    df1.createOrReplaceTempView("table1")
    Map(
      "names(1)" -> df1.withColumn("dummy", lit(null).cast("string")),
      "names(0)" -> df2.withColumn("dummy", lit(null).cast("string"))
    )
  }

//  override def process(inputTableNames: List[String], outputTableNames: List[String]): Unit = {
//    val df1 = spark.table(inputTableNames(0))
//    val df2 = spark.table(inputTableNames(1))
//
//    df1.withColumn("dummy", lit(null).cast("string")).createOrReplaceTempView(outputTableNames(0))
//    df2.withColumn("random", lit(null).cast("string")).createOrReplaceTempView(outputTableNames(1))
//  }

}
