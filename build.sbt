name := "scalaetl_vodqa"

version := "0.1"

scalaVersion := "2.12.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.192-R14"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.0"
// https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.0.jre8"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"