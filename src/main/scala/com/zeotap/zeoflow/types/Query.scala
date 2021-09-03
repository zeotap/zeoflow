package com.zeotap.zeoflow.types

class Query[A](query: String, name: String) extends Transformation[A] {
  def preprocess(inputTables: Map[String, A]): Map[String, A] = Map()
  def process(inputTables: Map[String, A], cache: Map[String, Any]): Map[String, A] = Map()
  def postprocess(inputTables: Map[String, A]): Map[String, A] = Map()
}
