package com.zeotap.zeoflow.common.flowexpectations

import com.zeotap.expectations.data.dsl.DataExpectation.ExpectationResult
import org.apache.spark.sql.DataFrame

object assertExpectationsUtils {

  def validateOutputDataExpectations(expectationOutput: Map[String, (DataFrame, Map[String, ExpectationResult])]): Boolean = {

    val assertionOutput: Map[String, List[Boolean]] = fetchDataExpectationIndividualResult(expectationOutput).foldLeft(Map.empty: Map[String, List[Boolean]])((accMap, res) => {
      accMap ++ Map(res._1 -> res._2.flatten)
    })
    assertionOutput.foldLeft(Map.empty: Map[String, Boolean])((acc, res) => {
      acc ++ Map(res._1 -> res._2.foldLeft(true)(_ && _))
    }).forall(dfResult => {
      true.equals(dfResult._2)
    })
  }

  private def fetchDataExpectationIndividualResult(aggregatedResult: Map[String, (DataFrame, Map[String, ExpectationResult])]) = {
    aggregatedResult.foldLeft(Map.empty: Map[String, List[List[Boolean]]])((accMap, output) => {
      val validationList: List[List[Boolean]] = output._2._2.foldLeft(List.empty: List[List[Boolean]])((accList, expRes) => {
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
