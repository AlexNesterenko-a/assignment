name := "task"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.11" % Test,
  ("org.apache.spark" %% "spark-core" % "2.4.3"),
  ("org.apache.spark" %% "spark-sql" % "2.4.3"),
  ("com.typesafe" % "config" % "1.3.3")
)
