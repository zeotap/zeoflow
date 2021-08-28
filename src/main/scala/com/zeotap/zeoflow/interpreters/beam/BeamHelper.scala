package com.zeotap.zeoflow.interpreters.beam

import com.zeotap.zeoflow.types.SQLQuery
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple, Row}

import scala.collection.mutable

object BeamHelper {
  implicit class BeamExt(pColMap: mutable.Map[String, PCollection[Row]]) {
    def createQueryContext() = buildPColTuple(pColMap.toList)

    def buildPColTuple(list: List[(String, PCollection[Row])]): PCollectionTuple = {
      list.size match {
        case 1 => PCollectionTuple.of(list.head._1, list.head._2)
        case _ => buildPColTuple(list.tail).and(list.head._1, list.head._2)
      }
    }

    def runSql(query: SQLQuery) = createQueryContext.apply(SqlTransform.query(query.query))

  }
}
