package com.zeotap.zeoflow.common.test.helpers

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import com.zeotap.expectations.column.dsl.{ColumnDSL, ColumnExpectation}
import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

object DataFrameUtils extends FunSuite with DataFrameSuiteBase {

  def assertDataFrameEquality(expectedDf: DataFrame, actualDf: DataFrame, sortColumn: String): Unit = {
    val expectedColumns = expectedDf.columns.sorted.map(col)
    val actualColumns = actualDf.columns.sorted.map(col)

    assertDataFrameEquals(expectedDf.select(expectedColumns : _*).distinct.orderBy(sortColumn),
      actualDf.select(actualColumns : _*).distinct.orderBy(sortColumn))
  }

  def collectResults[A, B](tables: List[String], parameters: List[Array[A]]): Map[String, B] = {

    def collectHelper[A, B](name: String, params: Array[A]) = params match {

      case columnExpectation if columnExpectation.isInstanceOf[Array[ColumnExpectation]] =>
        (name, ColumnDSL(columnExpectation.asInstanceOf[Array[ColumnExpectation]]: _ *).asInstanceOf[B])
    }
    (tables zip parameters).foldLeft(Map.empty[String, B]) { (map, element) =>
      map + collectHelper[A, B](element._1, element._2)
    }
  }

  def validateOutputDataExpectations(expectationOutput: Map[String, Map[String, ExpectationResult]]): Boolean = {

    val assertionOutput: Map[String, List[Boolean]] = fetchDataExpectationIndividualResult(expectationOutput).foldLeft(Map.empty: Map[String, List[Boolean]])((accMap, res) => {
      accMap ++ Map(res._1 -> res._2.flatten)
    })
    assertionOutput.foldLeft(Map.empty: Map[String, Boolean])((acc, res) => {
      acc ++ Map(res._1 -> res._2.foldLeft(true)(_ && _))
    }).forall(dfResult => {
      true.equals(dfResult._2)
    })
  }

  def fetchDataExpectationIndividualResult(aggregatedResult: Map[String, Map[String, ExpectationResult]]): Map[String, List[List[Boolean]]] = {
    aggregatedResult.foldLeft(Map.empty: Map[String, List[List[Boolean]]])((accMap, output) => {
      val validationList: List[List[Boolean]] = output._2.foldLeft(List.empty: List[List[Boolean]])((accList, expRes) => {
        val individualResultList: List[Boolean] = expRes._2.swap.value.foldLeft(List.empty: List[Boolean])((acc, individualRes) => {
          if (individualRes.isRight)
            acc ::: List(true)
          else
            acc ::: List(false)
        })
        accList ::: List(individualResultList)
      })
      accMap ++ Map(output._1 -> validationList)
    })
  }

}
