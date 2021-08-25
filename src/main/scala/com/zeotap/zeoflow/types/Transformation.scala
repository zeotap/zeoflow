package com.zeotap.zeoflow.types

trait Transformation[A]{
  def transform():Map[String,A]
}
