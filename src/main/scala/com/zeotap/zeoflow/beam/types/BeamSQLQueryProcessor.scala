package com.zeotap.zeoflow.beam.types

import com.zeotap.zeoflow.common.types.SQLQueryProcessor
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple, Row, TupleTag}

final case class BeamSQLQueryProcessor(name: String, query: String)(implicit beam: Pipeline) extends SQLQueryProcessor[PCollection[Row]](name, query) {
  override def preprocess(inputTables: Map[String, PCollection[Row]]): Map[String, PCollection[Row]] = inputTables
  override def process(inputTables: Map[String, PCollection[Row]], readOnlyGlobalCache: Map[String, Any]): Map[String, PCollection[Row]] = {
    val pCollectionTuple = inputTables.foldLeft(PCollectionTuple.empty(beam))((accTuple, table) => {
      accTuple.and(new TupleTag[Row](table._1), table._2)
    })
    val udfs = readOnlyGlobalCache("udfs").asInstanceOf[List[BeamUDF]]
    val sqlTransform = udfs.foldLeft(SqlTransform.query(query))((accTransform, udf) => {
      accTransform.registerUdf(udf.name, udf.function)
    })
    Map(name -> pCollectionTuple.apply(sqlTransform))
  }
  override def postprocess(outputTables: Map[String, PCollection[Row]]): Map[String, PCollection[Row]] = outputTables
}
