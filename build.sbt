name := "live-london-buses"

version := "0.1"

scalaVersion := "2.12.3"
val circeVersion = "0.8.0"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.9"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.github.mauricio" % "postgresql-async_2.12" % "0.2.21"
libraryDependencies += "net.debasishg" %% "redisclient" % "3.4"
libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.3"
libraryDependencies += "com.xebialabs.restito" % "restito" % "0.9.1" % "test"
libraryDependencies += "com.typesafe.play" %% "play-ahc-ws-standalone" % "1.0.7"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)
