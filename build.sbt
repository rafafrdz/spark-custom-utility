name := "spark-custom-utility"

version := "0.1"

scalaVersion := "2.12.11"
val sparkVersion = "2.4.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.30"
idePackagePrefix := Some("dev.rafafrdz")
