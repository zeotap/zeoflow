package com.zeotap.zeoflow.constructs

import com.zeotap.zeoflow.dsl.{SinkBuilder, SourceBuilder}
import com.zeotap.zeoflow.types.{ColumnAssertions, ColumnAssertionsProd, ColumnAssertionsQA, ProcessorTransformation, QueryTransformation, Transformation, UDF}
import org.apache.spark.sql.SparkSession
import com.zeotap.zeoflow.constructs.AssertionHelper.{prodAssertion, qaAssertions}

object FlowDslSparkOps {

  implicit class SparkExt(spark: SparkSession) {

    def loadSources(sources: List[SourceBuilder]): Unit = sources.foreach(source => source.build())

    def loadUserDefinedFunctions(udfs: List[UDF]): Unit = udfs.foreach(udf => spark.udf.register(udf.name, udf.function))

    def runTransformations(transformations: List[Transformation]): Unit =
      transformations.foreach {
        case qt: QueryTransformation => spark.sql(qt.query).createOrReplaceTempView(qt.name)
        case pt: ProcessorTransformation => pt.processor.process(pt.inputTableNames, pt.outputTableNames)
      }

    def writeToSinks(sinks: List[SinkBuilder]): Unit = sinks.foreach(sink => sink.build())

    def assertDataFrameExpectations(columnAssertions: List[ColumnAssertions]): Unit = {
      columnAssertions.foreach {
        case prod: ColumnAssertionsProd => prodAssertion(prod.dpName, prod.region, prod.productType, prod.inputTableNames)

        case qa: ColumnAssertionsQA => qaAssertions(qa.dpName, qa.region, qa.productType, qa.minDataframeSize, qa.totalDFsRequired)

      }
    }
  }

}
