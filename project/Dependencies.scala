import Versions._
import sbt._

object Dependencies {

  lazy val common = Seq(
    "org.apache.commons" % "commons-math3" % Versions.commonsMath,
    "commons-codec" % "commons-codec" % Versions.commonsCodec,
    "com.fasterxml.jackson.core" % "jackson-core" % Versions.jackson % Provided,
    "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jackson % Provided,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jackson % Provided
  )

  lazy val spark = Seq(
    "org.apache.spark" %% "spark-core" % Versions.spark % Provided,
    "org.apache.spark" %% "spark-sql" % Versions.spark % Provided,
    "org.apache.spark" %% "spark-graphx" % Versions.spark % Provided,
    "org.apache.spark" %% "spark-mllib" % Versions.spark % Provided,
    "org.apache.spark" %% "spark-hive" % Versions.spark % Provided
  )

  lazy val misc = Seq(
    "org.jgrapht" % "jgrapht-core" % Versions.jgrapht,
    "org.json" % "json" % Versions.json,
    "ch.qos.reload4j" % "reload4j" % Versions.reload4j,
    "org.apache.livy" %% "livy-core" % Versions.livy exclude("com.esotericsoftware.kryo", "kryo"),
    "org.scalameta" % "semanticdb-scalac_2.12.8" % Versions.semanticDb
  )

  lazy val testdeps = Seq(
    "com.holdenkarau" %% "spark-testing-base" % Versions.sparkTest,
    "org.scalatest" %% "scalatest" % Versions.scalaTest,
    "org.scalactic" %% "scalactic" % Versions.scalaTest
  )
}
