# Zeoflow

### A Free based DSL that models the high-level architecture of data transformation workflows

## Why
As we all know, any data workflow comes down to three major steps - Read, Transform and Write. Keeping that in mind, we have created a generic DSL (FlowDSL) that can be used for any data workload. Currently, only Spark is supported but the support for other frameworks like BigQuery, Beam, Kafka, etc. can be extended easily. This library also uses some of our other open sourced libraries like Data IO and Data Expectations which also work on the concept of a Free DSL and can also be extended to other frameworks easily.

## DSL
```
object FlowDSL {

  final case class LoadSources[A](sources: List[Source[A]]) extends FlowDSL[A]

  final case class LoadUserDefinedFunctions[A](udfs: List[FlowUDF[A]]) extends FlowDSL[A]

  final case class RunTransformations[A](transformations: List[Transformation[A]]) extends FlowDSL[A]

  final case class WriteToSinks[A](sinks: List[Sink[A]]) extends FlowDSL[A]

}
```

## Usage
Please go through the [Wiki](https://github.com/zeotap/zeoflow/wiki/Zeoflow) to understand the usage of the library.

## Build
Project is build using `sbt`

## Contributing
See the [CONTRIBUTING](/CONTRIBUTING.md) file for how to help out.

