package com.zeotap.zeoflow.common.types

abstract class SQLQueryProcessor[A](name: String, query: String) extends Transformation[A]
