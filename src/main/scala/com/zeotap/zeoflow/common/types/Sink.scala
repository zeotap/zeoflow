package com.zeotap.zeoflow.common.types

trait Sink[A] {
  def write(inputTables: Map[String, A]): Unit
}
