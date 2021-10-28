name := "zeoflow"

organization := "com.zeotap"

scalaVersion := "2.11.12"

import ReleaseTransformations._

libraryDependencies ++= Seq(
  "com.zeotap" % "cloud-storage-utils" % "1.0.0",
  "mysql" % "mysql-connector-java" % "8.0.26",
  "org.apache.commons" % "commons-text" % "1.6",
  "org.postgresql" % "postgresql" % "42.2.11",
  "org.scala-lang" % "scala-library" % "2.11.12",
  "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "0.21.1",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0",
  "com.zeotap" %% "data-io" % "1.0",
  "com.zeotap" %% "data-expectations" % "1.2",
  "org.apache.spark" %% "spark-avro" % "2.4.3",
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-hive" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.typelevel" %% "cats-core" % "2.0.0",
  "org.typelevel" %% "cats-free" % "2.0.0",
  "org.mockito" % "mockito-core" % "2.8.9" % Test,
  "org.testcontainers" % "mysql" % "1.16.0" % Test,
  "org.testcontainers" % "postgresql" % "1.16.0" % Test

)

fork in Test := true

javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")

parallelExecution in Test := false

credentials += Credentials(new File(Path.userHome.absolutePath + "/.sbt/.credentials"))

resolvers += "Artifactory Release" at "https://zeotap.jfrog.io/zeotap/libs-release"
resolvers += "Artifactory Snapshot" at "https://zeotap.jfrog.io/zeotap/libs-snapshot"
resolvers += Resolver.mavenLocal

publishTo := {
  val nexus = "https://zeotap.jfrog.io/zeotap/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "libs-snapshot-local")
  else
    Some("releases"  at nexus + "libs-release-local")
}

publishConfiguration := publishConfiguration.value.withOverwrite(true)

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
