package com.zeotap.zeoflow.types

trait Processor {

  def process(inputTableNames: List[String], outputTableNames: List[String]): Unit

}
