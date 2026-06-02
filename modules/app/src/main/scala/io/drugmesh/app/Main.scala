package io.drugmesh.app

import cats.syntax.all._
import com.monovore.decline.{CommandApp, Opts}

/**
 * CLI entrypoint. Replaces the original `CreateDrugMappings.main`, where enrichment passes
 * were toggled by editing source; here behavior is selected by subcommand and configuration.
 *
 *   drugmesh build-mappings    [--config conf/pipeline.conf]
 *   drugmesh resolve           [--config ...] [--threshold 0.9]
 *   drugmesh detect-anomalies  [--config ...]
 *   drugmesh index             [--config ...]
 */
object Main
    extends CommandApp(
      name = "drugmesh",
      header = "Spark-scale, explainable data-quality & entity-resolution engine for drug reference data.",
      main = {
        val config: Opts[String] =
          Opts
            .option[String]("config", short = "c", help = "Path to pipeline.conf (HOCON).")
            .withDefault("conf/pipeline.conf")

        val buildMappings =
          Opts.subcommand("build-mappings", "Run the enrichment DAG and write drug-mappings.tsv.") {
            config.map(Actions.buildMappings)
          }

        val resolve =
          Opts.subcommand("resolve", "Probabilistic entity resolution over the mapping table.") {
            (
              config,
              Opts.option[Double]("threshold", help = "Posterior match threshold.").withDefault(0.90)
            ).mapN(Actions.resolve)
          }

        val detectAnomalies =
          Opts.subcommand("detect-anomalies", "Isolation-forest data-quality anomaly scoring.") {
            config.map(Actions.detectAnomalies)
          }

        val index =
          Opts.subcommand("index", "Bulk-index the resolved table into Elasticsearch.") {
            config.map(Actions.indexEs)
          }

        buildMappings orElse resolve orElse detectAnomalies orElse index
      }
    )
