scalaVersion := "2.11.12"

ensimeScalaVersion in ThisBuild := "2.11.12"

name := "XSocial"

version := "0.1.0"

val sparkVersion = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)
