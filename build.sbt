// =====================================================================================
// DrugMesh — a Spark-scale, explainable data-quality & entity-resolution engine
// for pharmaceutical reference data.
//
// Multi-module sbt build. Target platform: Scala 2.13 + Apache Spark 3.5.x (LTS).
// Spark application code requires Scala 2.13 (Spark 4 dropped 2.12; Spark does not
// support Scala 3 for application code), so 2.13 is the deliberate, forward-compatible
// target whether you stay on the 3.5 LTS line or move to Spark 4.0.
// =====================================================================================

// Pinned dependency versions (single source of truth as top-level vals).
val scalaV          = "2.13.14"
val sparkV          = "3.5.3" // 3.5 LTS line; bump to "4.0.1" for Spark 4 (still Scala 2.13).
val sttpV           = "4.0.3"
val circeV          = "0.14.10"
val catsEffectV     = "3.5.4"
val catsV           = "2.12.0"
val scalaXmlV       = "2.3.0"
val pureconfigV     = "0.17.7"
val declineV        = "2.4.1"
val logbackV        = "1.5.8"
val scalaLoggingV   = "3.9.5"
val munitV          = "1.0.2"
val scalacheckV     = "1.18.0"
val sparkNlpV       = "5.5.0"
val isolationForestV = "3.0.5" // LinkedIn distributed Isolation Forest, Spark/Scala native.
val esSparkV        = "8.15.3"

ThisBuild / organization := "io.drugmesh"
ThisBuild / version      := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := scalaV

// Run forked so Spark's reflective access and shutdown hooks behave.
ThisBuild / Test / fork := true
ThisBuild / Test / javaOptions ++= Seq(
  "-Xmx2g",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

lazy val commonScalacOptions = Seq(
  "-encoding", "utf-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint:_",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Wunused:imports"
)

// Spark is `Provided`: it is supplied by the cluster at spark-submit time and is kept
// off the assembled fat jar. The `app` module re-enables it on the local run classpath
// (see `runWithProvided` below) so `sbt app/run` works without a cluster.
lazy val sparkDeps = Seq(
  "org.apache.spark" %% "spark-core" % sparkV % Provided,
  "org.apache.spark" %% "spark-sql"  % sparkV % Provided
)

lazy val sparkMlDeps = Seq(
  "org.apache.spark" %% "spark-mllib" % sparkV % Provided
)

// Lets `run` see `Provided` deps (Spark) locally without polluting the published artifact.
lazy val runWithProvided = Seq(
  Compile / run := Defaults
    .runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner)
    .evaluated,
  Compile / runMain := Defaults
    .runMainTask(Compile / fullClasspath, Compile / run / runner)
    .evaluated,
  Compile / run / fork := true
)

lazy val commonSettings = Seq(
  scalacOptions ++= commonScalacOptions,
  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging"    % scalaLoggingV,
    "ch.qos.logback"              % "logback-classic"  % logbackV % Runtime,
    "org.scalameta"             %% "munit"             % munitV      % Test,
    "org.scalameta"             %% "munit-scalacheck"  % munitV      % Test,
    "org.scalacheck"            %% "scalacheck"        % scalacheckV % Test
  ),
  testFrameworks += new TestFramework("munit.Framework")
)

// -------------------------------------------------------------------------------------
// Modules
// -------------------------------------------------------------------------------------

// core — pure domain model + normalization. Encoders are derived at use sites via
// `import spark.implicits._`, so core carries no Spark runtime usage.
lazy val core = (project in file("modules/core"))
  .settings(commonSettings)
  .settings(
    name := "drugmesh-core",
    libraryDependencies ++= sparkDeps ++ Seq(
      "org.typelevel" %% "cats-core" % catsV
    )
  )

// ingest — source parsers (DrugBank XML, TTD/KEGG/STITCH TSV) -> Dataset[DrugEntry].
lazy val ingest = (project in file("modules/ingest"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "drugmesh-ingest",
    libraryDependencies ++= sparkDeps ++ Seq(
      "org.scala-lang.modules" %% "scala-xml" % scalaXmlV
    )
  )

// clients — typed, effectful external API clients (ChEMBL/UniChem, PubChem, UMLS, DGIdb).
lazy val clients = (project in file("modules/clients"))
  .dependsOn(core)
  .settings(commonSettings)
  .settings(
    name := "drugmesh-clients",
    libraryDependencies ++= Seq(
      "com.softwaremill.sttp.client4" %% "core"          % sttpV,
      "com.softwaremill.sttp.client4" %% "cats"          % sttpV,
      "io.circe"                      %% "circe-core"    % circeV,
      "io.circe"                      %% "circe-generic" % circeV,
      "io.circe"                      %% "circe-parser"  % circeV,
      "org.typelevel"                 %% "cats-effect"   % catsEffectV
    )
  )

// pipeline — typed enrichment DAG that replaces the original imperative CreateDrugMappings.
lazy val pipeline = (project in file("modules/pipeline"))
  .dependsOn(core, ingest, clients)
  .settings(commonSettings)
  .settings(
    name := "drugmesh-pipeline",
    libraryDependencies ++= sparkDeps ++ Seq(
      "com.github.pureconfig" %% "pureconfig" % pureconfigV
    )
  )

// ml — the six ML / text-mining extensions. Kept separate so the ETL compiles without
// the heavy ML deps. Spark NLP / ES connector / isolation-forest are Spark-native.
lazy val ml = (project in file("modules/ml"))
  .dependsOn(core, pipeline)
  .settings(commonSettings)
  .settings(
    name := "drugmesh-ml",
    libraryDependencies ++= sparkDeps ++ sparkMlDeps ++ Seq(
      "com.johnsnowlabs.nlp"          %% "spark-nlp"              % sparkNlpV,
      "com.linkedin.isolation-forest" %% "isolation-forest_3.5.0" % isolationForestV,
      "org.elasticsearch"             %% "elasticsearch-spark-30" % esSparkV
    )
  )

// app — CLI entrypoint wiring the pipeline + ml stages together.
lazy val app = (project in file("modules/app"))
  .dependsOn(pipeline, ml)
  .settings(commonSettings)
  .settings(runWithProvided)
  .settings(
    name := "drugmesh-app",
    libraryDependencies ++= sparkDeps ++ sparkMlDeps ++ Seq(
      "com.monovore" %% "decline" % declineV
    ),
    Compile / mainClass := Some("io.drugmesh.app.Main")
  )

// Aggregate root.
lazy val root = (project in file("."))
  .aggregate(core, ingest, clients, pipeline, ml, app)
  .settings(
    name := "drugmesh",
    publish / skip := true
  )
