package com.zeotap.zeoflow.dsl

trait SourceBuilder[A] {
  def build(): A
}
