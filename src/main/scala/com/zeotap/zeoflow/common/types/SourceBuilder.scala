package com.zeotap.zeoflow.common.types

trait SourceBuilder[A] {
  def build(): (String, A)
}
