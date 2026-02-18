// ======================================================
// Aurora Core - build.sbt (Scala 3 Production Setup)
// ======================================================

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.1"
ThisBuild / organization := "com.aurora"

// Disable forking to keep CLI alive
fork := false
// ------------------------------------------------------
// Scala 3 Compiler Options
// ------------------------------------------------------
ThisBuild / scalacOptions ++= Seq(
  "-Xignore-scala2-macros",
  "-Xsource:3-cross",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-source:future"
)

lazy val akkaVersion       = "2.8.5"
lazy val akkaHttpVersion   = "10.5.2"
lazy val circeVersion      = "0.14.6"
lazy val mongoVersion      = "4.11.1"

lazy val root = (project in file("."))
  .settings(
    name := "aurora-core",

    // --------------------------------------------------
    // Dependencies
    // --------------------------------------------------
    libraryDependencies ++= Seq(

      // ===== Akka (Scala 3 via 2.13 cross-build) =====
      "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion cross(CrossVersion.for3Use2_13),
      "com.typesafe.akka" %% "akka-stream"      % akkaVersion cross(CrossVersion.for3Use2_13),
      "com.typesafe.akka" %% "akka-http"        % akkaHttpVersion cross(CrossVersion.for3Use2_13),
      "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion % Test cross(CrossVersion.for3Use2_13),

      // ===== MongoDB (Sync Driver) =====
      "org.mongodb" % "mongodb-driver-sync" % mongoVersion,

      // ===== JSON (Circe) =====
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion,

      // ===== JWT =====
      "com.github.jwt-scala" %% "jwt-circe" % "9.4.5",

      // ===== Config =====
      "com.typesafe" % "config" % "1.4.3",

      // ===== Logging =====
      "org.slf4j" % "slf4j-api" % "2.0.9",
      "ch.qos.logback" % "logback-classic" % "1.4.14",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5" cross(CrossVersion.for3Use2_13),

      // ===== Testing =====
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    ),

    // --------------------------------------------------
    // Runtime Settings
    // --------------------------------------------------
    fork := true,

    javaOptions ++= Seq(
      "-Xms256M",
      "-Xmx1G",
      "-XX:+UseG1GC",
      "-Duser.timezone=UTC"
    )
  )
