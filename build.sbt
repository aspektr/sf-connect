name := "sf-connect"
organization := "azurdrive"
version := "0.2"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.softwaremill.sttp.client" %% "core" % "2.2.5",
  "com.softwaremill.sttp.client" %% "circe" % "2.2.5"
)

val circeVersion = if (scalaVersion == "2.13.3")  "0.12.3" else "0.11.2"


libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

assemblyJarName in assembly := "sf-connect-2.jar"
