import Dependencies._

scalaVersion := Versions.scalaVersion

name := "spark_er"
organization := "org.scify"
version := "1.0"

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(rootSettings)
  .settings(MergeRules.settings)
  .settings(
    description := "Document and Entity Clustering Library",
    libraryDependencies ++= common ++ spark ++ misc ++ testdeps,
    assembly / test  := {},
    Test / fork := true
  )

lazy val rootSettings = Seq(
  concurrentRestrictions := Seq(
    Tags.limit(Tags.CPU, java.lang.Runtime.getRuntime.availableProcessors()),
    // limit to 1 concurrent test task, even across sub-projects
    Tags.limitSum(1, Tags.Test, Tags.Untagged))
)

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization := "org.scify",
  crossPaths := true,
  scalaVersion := Versions.scalaVersion,
  publishArtifact := false,
  Test / parallelExecution := false,

  // We need to exclude jms/jmxtools/etc because it causes undecipherable SBT errors  :(
  ivyXML :=
    <dependencies>
      <exclude module="jms"/>
      <exclude module="jmxtools"/>
      <exclude module="jmxri"/>
    </dependencies>

//  resolvers ++= Seq(
//    DefaultMavenRepository,
//    Resolver.sonatypeOssRepos("releases"),
//    Resolver.mavenCentral
//    // Resolver.typesafeRepo("releases")
//	)
)
