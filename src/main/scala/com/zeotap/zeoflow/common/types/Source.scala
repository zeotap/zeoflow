package com.zeotap.zeoflow.common.types

trait Source[A] {
  def load(): (String, A)
}
