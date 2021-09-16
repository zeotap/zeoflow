package com.zeotap.zeoflow.common.types

trait FlowUDF[A] {
  def register(): A
}
