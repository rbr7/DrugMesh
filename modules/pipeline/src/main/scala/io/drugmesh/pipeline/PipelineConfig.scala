package io.drugmesh.pipeline

import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.duration.FiniteDuration

/**
 * Typed configuration loaded from `conf/pipeline.conf` (HOCON) via PureConfig. This replaces
 * the booleans that were commented in/out of the original `CreateDrugMappings.main()`:
 * enrichment passes are now declared as data and can be reordered without recompiling.
 *
 * PureConfig's default mapping turns camelCase fields into kebab-case keys, matching the
 * config file (`app-name`, `shuffle-partitions`, ...).
 */
final case class SparkConf(
    appName: String,
    master: String,
    shufflePartitions: Int,
    adaptiveEnabled: Boolean
)

final case class IoConf(
    drugbankXml: String,
    ttdRaw: String,
    keggDrug: String,
    stitchSider: String,
    dgidbDrugs: String,
    outputTsv: String,
    apiCacheDir: String
)

final case class EnrichmentConf(passes: List[String])

final case class UmlsConf(base: String, authBase: String, apiKey: Option[String])

final case class ClientsConf(
    userAgent: String,
    maxRetries: Int,
    baseBackoff: FiniteDuration,
    requestsPerSec: Int,
    chemblBase: String,
    pubchemBase: String,
    dgidbBase: String,
    umls: UmlsConf
)

final case class EntityResolutionConf(
    blockingKeys: List[String],
    matchThreshold: Double,
    reviewLower: Double
)
final case class AnomalyConf(contamination: Double, numEstimators: Int, maxSamples: Int)
final case class SearchConf(esNodes: String, esPort: Int, indexName: String)
final case class EmbeddingsConf(model: String, dimension: Int)
final case class MlConf(
    entityResolution: EntityResolutionConf,
    anomaly: AnomalyConf,
    search: SearchConf,
    embeddings: EmbeddingsConf
)

final case class DrugMeshConfig(
    spark: SparkConf,
    io: IoConf,
    enrichment: EnrichmentConf,
    clients: ClientsConf,
    ml: MlConf
)

object DrugMeshConfig {

  /** Load and validate from the default config sources (reference.conf + application.conf). */
  def load(): Either[String, DrugMeshConfig] =
    ConfigSource.default
      .at("drugmesh")
      .load[DrugMeshConfig]
      .left
      .map(_.prettyPrint())

  /** Load from an explicit file path (e.g. `conf/pipeline.conf`). */
  def loadFile(path: String): Either[String, DrugMeshConfig] =
    ConfigSource
      .file(path)
      .at("drugmesh")
      .load[DrugMeshConfig]
      .left
      .map(_.prettyPrint())
}
