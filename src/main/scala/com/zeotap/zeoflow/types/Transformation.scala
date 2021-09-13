package com.zeotap.zeoflow.types

trait Transformation[A] {
  def preprocess(inputTables: Map[String, A]): Map[String, A]
  def process(inputTables: Map[String, A], cache: Map[String, Any]): Map[String, A]
  def postprocess(outputTables: Map[String, A]): Map[String, A]
}
