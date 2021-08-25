package com.zeotap.zeoflow.types

trait Processor[A] {
  def process(inputTableNames: Map[String,A], cache:Map[String,Any]): Map[String,A]

}

/*
 P1(implicit spark) extends P
 def process1(inputTableNames: List[String]): A

 P1(implicit beam) extends P
 def process1(inputTableNames: List[String]): A


 */