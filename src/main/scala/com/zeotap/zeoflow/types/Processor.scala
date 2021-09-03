package com.zeotap.zeoflow.types

trait Processor[A] extends Transformation[A] {
  def preprocess(inputTables: Map[String, A]): Map[String, A]
  def process(inputTables: Map[String, A], cache: Map[String, Any]): Map[String, A]
  def postprocess(inputTables: Map[String, A]): Map[String, A]
}
