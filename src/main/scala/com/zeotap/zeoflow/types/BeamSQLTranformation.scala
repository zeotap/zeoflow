package com.zeotap.zeoflow.types

import org.apache.beam.sdk.values.{PCollection, Row}

case class BeamSQLTransformation(query: SQLQuery) extends SQLTransformation[PCollection[Row]](query){
  override def transform(): Map[String, PCollection[Row]] = ???
}
