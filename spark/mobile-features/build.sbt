name := "mobile-features"

version := "0.3"

scalaVersion := "2.12.15"
val sparkVersion = "3.2.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

idePackagePrefix := Some("net.lirneasia")