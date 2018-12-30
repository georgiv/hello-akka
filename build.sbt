name := "hello-akka"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.19",
  "com.typesafe.akka" %% "akka-stream" % "2.5.19",
  "com.typesafe.akka" %% "akka-http" % "10.1.5",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.5",

  "mysql" % "mysql-connector-java" % "8.0.13",

  "org.scalikejdbc" %% "scalikejdbc" % "3.3.1",
  "com.h2database" %  "h2" % "1.4.197",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
