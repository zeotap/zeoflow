package com.zeotap.zeoflow.types

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

class CustomProcessor extends Processor[DataFrame] {

  override def process(inputTableNames: Map[String, DataFrame], cache:Map[String,Any]): Map[String, DataFrame] = {
    val df1 = inputTableNames("name1")//spark.table(inputTableNames(0))
    val df2 = inputTableNames("name2")//spark.table(inputTableNames(1))

    //df1.createOrReplaceTempView("table1")
    Map(
      "names(1)" -> df1.withColumn("dummy", lit(null).cast("string")),
      "names(0)" -> df2.withColumn("dummy", lit(null).cast("string"))
    )
  }
}
