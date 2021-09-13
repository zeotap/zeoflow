package com.zeotap.zeoflow.types

abstract class SQLQueryProcessor[A](name: String, query: String) extends Transformation[A]
