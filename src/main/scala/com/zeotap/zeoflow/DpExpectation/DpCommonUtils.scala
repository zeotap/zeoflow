package com.zeotap.zeoflow.DpExpectation

import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.utility.spark.types.{DataColumn, SparkDataframe}

object DpCommonUtils {

  def wrapResult[A, B](colNames: List[String], params: List[Array[A]]): Map[String, B] = {

    def helper[A, B](name: String, params: Array[A]) = params match {

      case dataColumn if dataColumn.isInstanceOf[Array[DataColumn]] =>
        (name, SparkDataframe(dataColumn.asInstanceOf[Array[DataColumn]]: _*).asInstanceOf[B])
      case columnExpectation if columnExpectation.isInstanceOf[Array[ColumnExpectation]] =>
        (name, ColumnDSL(columnExpectation.asInstanceOf[Array[ColumnExpectation]]: _ *).asInstanceOf[B])
    }

    (colNames zip params).foldLeft(Map.empty[String, B]) { (map, element) =>
      map + helper[A, B](element._1, element._2)
    }
  }

}
