package com.zeotap.zeoflow.common.types

trait SinkBuilder[A] {
  def build(inputTables: Map[String, A]): Unit
}
