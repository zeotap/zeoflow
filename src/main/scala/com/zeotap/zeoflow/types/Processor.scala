package com.zeotap.zeoflow.types

trait Processor[A] {
  def process(inputTableNames: Map[String,A], cache:Map[String,Any]): Map[String,A]
}