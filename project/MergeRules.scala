import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys.{ assembly, assemblyMergeStrategy}
import sbtassembly.AssemblyPlugin.autoImport.MergeStrategy

object MergeRules {
  lazy val settings = Seq(

    // uncomment below to exclude tests
    assembly / test := {},

    // We don't need the Scala library, Spark already includes it
    assembly / assemblyMergeStrategy  := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
