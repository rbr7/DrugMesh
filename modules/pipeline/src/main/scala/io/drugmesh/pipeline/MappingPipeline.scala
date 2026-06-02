package io.drugmesh.pipeline

import cats.effect.unsafe.implicits.global
import com.typesafe.scalalogging.LazyLogging
import io.drugmesh.clients._
import io.drugmesh.core.DrugEntry
import io.drugmesh.ingest.{DrugBankXmlParser, TsvSources}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration._

/**
 * Orchestrates the enrichment DAG. Replaces the imperative `CreateDrugMappings.main()` and
 * its toggled passes: the ordered `passes` list from config is folded over a seed dataset,
 * each name dispatching to a typed transformation. A single HTTP backend is allocated for the
 * whole run and released deterministically at the end.
 */
object MappingPipeline extends LazyLogging {

  def run(cfg: DrugMeshConfig)(implicit spark: SparkSession): Dataset[DrugEntry] = {
    val policy      = RetryPolicy(cfg.clients.maxRetries, cfg.clients.baseBackoff)
    val concurrency = math.max(1, cfg.clients.requestsPerSec)
    val minInterval = (1000L / concurrency).millis

    val (backend, release) = HttpSupport.backend.allocated.unsafeRunSync()
    try {
      val ua = cfg.clients.userAgent
      val clients = ApiClients(
        chembl = new ChemblClient(cfg.clients.chemblBase, backend, ua, policy),
        pubchem = new PubChemClient(cfg.clients.pubchemBase, backend, ua, policy),
        umls = new UmlsClient(cfg.clients.umls.base, cfg.clients.umls.authBase, cfg.clients.umls.apiKey.getOrElse(""), backend, ua, policy),
        dgidb = new DGIdbClient(cfg.clients.dgidbBase, backend, ua, policy)
      )

      val seed = { import spark.implicits._; spark.emptyDataset[DrugEntry] }
      val result = cfg.enrichment.passes.foldLeft(seed) { (acc, pass) =>
        logger.info(s"applying enrichment pass: $pass")
        applyPass(pass, acc, cfg, clients, concurrency, minInterval)
      }
      result
    } finally release.unsafeRunSync()
  }

  private def applyPass(
      pass: String,
      acc: Dataset[DrugEntry],
      cfg: DrugMeshConfig,
      clients: ApiClients,
      concurrency: Int,
      minInterval: FiniteDuration
  )(implicit spark: SparkSession): Dataset[DrugEntry] = {
    val cache = cfg.io.apiCacheDir
    pass match {
      case "drugbank-base" =>
        DrugBankXmlParser.loadEntries(spark, cfg.io.drugbankXml)

      case "ttd-merge" =>
        Enrichment.mergeTtd(acc, TsvSources.readTtd(spark, cfg.io.ttdRaw))

      case "chembl-unichem" =>
        val lookup = ApiEnrichment.resolveChemblByDrugBank(acc, clients.chembl, cache, concurrency, minInterval)
        Enrichment.applyLookup(acc, lookup)(ApiEnrichment.setChembl)

      case "pubchem-pugrest" =>
        val lookup = ApiEnrichment.resolvePubChemByName(acc, clients.pubchem, cache, concurrency, minInterval)
        Enrichment.applyLookup(acc, lookup)(ApiEnrichment.setPubChem)

      case "kegg-file" =>
        Enrichment.mergeKegg(acc, TsvSources.readKegg(spark, cfg.io.keggDrug))

      case "umls-cuis" =>
        val lookup = ApiEnrichment.resolveUmlsByName(acc, clients.umls, cache, concurrency, minInterval)
        Enrichment.applyLookup(acc, lookup)(ApiEnrichment.setUmls)

      case "stitch-sider" =>
        Enrichment.mergeStitch(acc, TsvSources.readStitchSider(spark, cfg.io.stitchSider))

      case "dgidb" =>
        // DGIdb is used as an independent corroboration signal rather than a primary id
        // source; the cross-check is reported by the anomaly/explainability stages.
        logger.info("dgidb pass: corroboration only, no field overwrite")
        acc

      case other =>
        logger.warn(s"unknown enrichment pass '$other' — skipping")
        acc
    }
  }
}
