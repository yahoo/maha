// Copyright 2018, Oath Inc.
// Licensed under the terms of the Apache License 2.0. Please see LICENSE file in project root for terms.
name := "druid-manager"

version := "1.1"

scalaVersion := "2.12.6"

resolvers += "Y! BinTray Artifactory" at "http://yahoo.bintray.com/maven"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/play"

resolvers += "Typesafe Simple Repository" at "http://repo.typesafe.com/typesafe/simple/maven-releases/"

libraryDependencies ++= Seq(
  ws
)

libraryDependencies += jdbc


libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.1.0"

libraryDependencies += "com.google.guava" % "guava" % "26.0-jre"

libraryDependencies += "org.webjars" % "jquery" % "3.3.1-1"

libraryDependencies +=filters

libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "com.yahoo.maha" % "maha-core" % "5.194"

libraryDependencies += "com.yahoo.maha" % "maha-druid-executor" % "5.185"

libraryDependencies += "org.bouncycastle" % "bcpkix-jdk15on" % "1.53"

libraryDependencies += "org.bouncycastle" % "bcprov-jdk15on" % "1.53"

libraryDependencies += "com.zaxxer" % "HikariCP" % "3.2.0"

libraryDependencies += "javax.servlet" % "servlet-api" % "2.5"

unmanagedBase <<= baseDirectory { base => base / "app" /"lib" }

lazy val root = (project in file(".")).enablePlugins(PlayScala, RpmPlugin, RpmDeployPlugin)

routesGenerator := InjectedRoutesGenerator

fork in run := false

maintainer := "Yahoo <yahoo@example.com>"

packageSummary := "A tool for managing Apache Druid cluster"

packageDescription := "A tool for managing Apache Druid cluster"

rpmRelease := "1"

version in Rpm := sys.props.getOrElse("version", default = "1.0.0")

rpmVendor := "yahoo"

rpmUrl := Some("https://github.com/yahoo/maha/tree/master/druid-manager")

rpmLicense := Some("Apache v2")

rpmBrpJavaRepackJars := true

rpmGroup := Some("druid-manager")
