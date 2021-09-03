package com.zeotap.zeoflow.types

case class ProcessorTransformation(processor: Processor, inputTableNames: List[String], outputTableNames: List[String]) extends Transformation
