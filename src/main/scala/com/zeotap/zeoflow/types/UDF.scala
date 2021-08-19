package com.zeotap.zeoflow.types

import org.apache.spark.sql.expressions.UserDefinedFunction

case class UDF(function: UserDefinedFunction, name: String)
