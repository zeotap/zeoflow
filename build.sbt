name := "zeoflow"

organization := "com.zeotap"

scalaVersion := "2.12.14"

import ReleaseTransformations._

val sparkVersion = "3.1.2"
val beamVersion = "2.33.0"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.module" % "jackson-module-paranamer" % "2.12.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.1",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.21.1",
  "com.zeotap" %% "spark-property-tests" % "3.1.2",
  "com.zeotap" %% "data-expectations" % "1.3-SNAPSHOT",
  "com.zeotap" %% "data-io" % "1.1",
  "mysql" % "mysql-connector-java" % "8.0.26",
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-extensions-sql" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-io-jdbc" % beamVersion,
  "org.apache.beam" % "beam-sdks-java-io-parquet" % beamVersion,
  "org.apache.commons" % "commons-text" % "1.6",
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.2.11",
  "org.scala-lang" % "scala-library" % "2.12.14",
  "org.typelevel" %% "cats-core" % "2.0.0",
  "org.typelevel" %% "cats-free" % "2.0.0",
  "org.mockito" % "mockito-core" % "2.8.9" % Test,
  "org.testcontainers" % "mysql" % "1.16.0" % Test,
  "org.testcontainers" % "postgresql" % "1.16.0" % Test

)

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

releaseTagComment    := s" Releasing ${(version in ThisBuild).value}"
releaseCommitMessage := s"[skip ci] Setting version to ${(version in ThisBuild).value}"
releaseNextCommitMessage := s"[skip ci] Setting version to ${(version in ThisBuild).value}"

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion
)
