package com.zeotap.zeoflow.dsl.source

trait Source[A] {
  def load(): A
}


