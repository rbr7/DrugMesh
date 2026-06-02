package io.drugmesh.app

import com.typesafe.scalalogging.LazyLogging
import io.drugmesh.core.DrugEntry
import io.drugmesh.ingest.SparkSupport
import io.drugmesh.ml.anomaly.AnomalyDetection
import io.drugmesh.ml.entityres.{EntityResolution, ResolutionRecord}
import io.drugmesh.ml.explain.MatchExplanation
import io.drugmesh.ml.search.ElasticIndexer
import io.drugmesh.pipeline.{DrugMeshConfig, MappingPipeline, TsvWriter}
import org.apache.spark.sql.{Dataset, SparkSession}

/** Concrete actions behind each CLI subcommand. Each loads config, opens a Spark session,
 * does its work, and stops the session. */
object Actions extends LazyLogging {

  /** Run the enrichment DAG and write the legacy-format drug-mappings.tsv. */
  def buildMappings(configPath: String): Unit = withConfigAndSpark(configPath) { (cfg, spark) =>
    implicit val s: SparkSession = spark
    val mapped = MappingPipeline.run(cfg)
    TsvWriter.writeLocal(mapped, cfg.io.outputTsv)
    println(s"Wrote ${mapped.count()} drug records to ${cfg.io.outputTsv}")
  }

  /** Resolve duplicate/co-referent records in the mapping table and print the top matches. */
  def resolve(configPath: String, threshold: Double): Unit = withConfigAndSpark(configPath) { (cfg, spark) =>
    implicit val s: SparkSession = spark
    val records = resolutionRecords(spark, cfg.io.outputTsv)
    val matches = EntityResolution.resolve(
      records,
      matchThreshold = threshold,
      reviewLower = cfg.ml.entityResolution.reviewLower
    )
    val top = matches.collect().sortBy(-_.posterior).take(25)
    println(s"Found ${matches.count()} candidate links above review threshold. Top:")
    top.foreach(m => println("  " + MatchExplanation.summary(m)))
  }

  /** Score every row for data-quality anomalies and print the worst offenders. */
  def detectAnomalies(configPath: String): Unit = withConfigAndSpark(configPath) { (cfg, spark) =>
    implicit val s: SparkSession = spark
    val entries = TsvWriter.readTsv(spark, cfg.io.outputTsv)
    val scored = AnomalyDetection.detect(
      entries,
      contamination = cfg.ml.anomaly.contamination,
      numEstimators = cfg.ml.anomaly.numEstimators,
      maxSamples = cfg.ml.anomaly.maxSamples
    )
    println("Top data-quality anomalies (highest outlier score):")
    scored.orderBy(org.apache.spark.sql.functions.desc("outlier_score")).show(25, truncate = false)
  }

  /** Bulk-index the resolved table into Elasticsearch for fuzzy drug-name search. */
  def indexEs(configPath: String): Unit = withConfigAndSpark(configPath) { (cfg, spark) =>
    implicit val s: SparkSession = spark
    val entries = TsvWriter.readTsv(spark, cfg.io.outputTsv)
    ElasticIndexer.index(entries, cfg.ml.search)
    println(s"Indexed into Elasticsearch index '${cfg.ml.search.indexName}'")
  }

  // -- helpers ----------------------------------------------------------------------------

  private def resolutionRecords(spark: SparkSession, tsvPath: String): Dataset[ResolutionRecord] = {
    import spark.implicits._
    TsvWriter.readTsv(spark, tsvPath).map(e => ResolutionRecord(e, inchiKey = None))
  }

  private def withConfigAndSpark(configPath: String)(f: (DrugMeshConfig, SparkSession) => Unit): Unit =
    DrugMeshConfig.loadFile(configPath) match {
      case Left(err) =>
        System.err.println(s"Configuration error in $configPath:\n$err")
        sys.exit(2)
      case Right(cfg) =>
        val spark = SparkSupport.session(
          appName = cfg.spark.appName,
          master = Some(cfg.spark.master),
          shufflePartitions = cfg.spark.shufflePartitions,
          adaptive = cfg.spark.adaptiveEnabled
        )
        try f(cfg, spark)
        finally spark.stop()
    }
}
