import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.sagainfo",
      scalaVersion := "2.12.7",
      version      := "0.1.0"
    )),
    name := "rvAgent",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    libraryDependencies += "com.typesafe" % "config" % "1.3.3",
    libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.20.0",
    libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.19",
	scalacOptions ++= Seq("-feature")
  )

