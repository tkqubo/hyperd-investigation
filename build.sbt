name := "hyperd-investigation"

version := "0.1"

scalaVersion := "2.13.1"

unmanagedJars in Compile ++= Seq(baseDirectory.value / "lib" / "tableauhyperapi.jar")

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.0.0",
  "org.typelevel" %% "cats-effect" % "2.0.0",
  "org.scalatest" %% "scalatest" % "3.3.0-SNAP2" % Test
)
