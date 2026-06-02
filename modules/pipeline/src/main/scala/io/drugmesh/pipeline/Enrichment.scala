package io.drugmesh.pipeline

import io.drugmesh.core.{DrugEntry, Normalization}
import io.drugmesh.ingest.{KeggDrug, StitchMap, TtdDrug}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * The enrichment passes, each expressed as a typed transformation
 * `Dataset[DrugEntry] => Dataset[DrugEntry]`. This is the compositional replacement for the
 * 600+ line imperative `CreateDrugMappings`: instead of mutating shared `HashMap`s, every
 * pass left-joins a source and fills only the *missing* fields (existing values win), so the
 * passes are order-independent where the data allows and individually unit-testable.
 *
 * Joins are typed `joinWith` left-outer joins; small dimension tables should be broadcast by
 * the caller. Name-based joins use the salt-stripped normalized name as the key.
 */
object Enrichment {

  private def nameKey(s: String): String = Normalization.stripSalts(s)

  /** Merge Therapeutic Target Database ids by normalized name. */
  def mergeTtd(base: Dataset[DrugEntry], ttd: Dataset[TtdDrug])(implicit spark: SparkSession): Dataset[DrugEntry] = {
    import spark.implicits._
    val left  = base.map(d => (nameKey(d.name), d))
    val right = ttd.map(t => (nameKey(t.name), t))
    left
      .joinWith(right, left("_1") === right("_1"), "left")
      .map { case ((_, d), pair) =>
        val t = Option(pair).map(_._2)
        d.copy(
          ttdId      = d.ttdId.orElse(t.map(_.ttdId).filter(_.nonEmpty)),
          casNum     = d.casNum.orElse(t.flatMap(_.casNum)),
          pubchemCid = d.pubchemCid.orElse(t.flatMap(_.pubchemCid))
        )
      }
  }

  /** Merge KEGG ids/cross-references by normalized name. */
  def mergeKegg(base: Dataset[DrugEntry], kegg: Dataset[KeggDrug])(implicit spark: SparkSession): Dataset[DrugEntry] = {
    import spark.implicits._
    val left  = base.map(d => (nameKey(d.name), d))
    val right = kegg.map(k => (nameKey(k.name), k))
    left
      .joinWith(right, left("_1") === right("_1"), "left")
      .map { case ((_, d), pair) =>
        val k = Option(pair).map(_._2)
        d.copy(
          keggId     = d.keggId.orElse(k.map(_.keggId).filter(_.nonEmpty)),
          chebiId    = d.chebiId.orElse(k.flatMap(_.chebiId)),
          casNum     = d.casNum.orElse(k.flatMap(_.casNum)),
          pubchemCid = d.pubchemCid.orElse(k.flatMap(_.pubchemCid))
        )
      }
  }

  /** Attach STITCH ids from the DrugBank-SIDER mapping, joined on DrugBank id. */
  def mergeStitch(base: Dataset[DrugEntry], stitch: Dataset[StitchMap])(implicit spark: SparkSession): Dataset[DrugEntry] = {
    import spark.implicits._
    val left  = base.map(d => (d.drugbankId, d))
    val right = stitch.map(s => (s.drugbankId, s.stitchId))
    left
      .joinWith(right, left("_1") === right("_1"), "left")
      .map { case ((_, d), pair) =>
        d.copy(stitchId = d.stitchId.orElse(Option(pair).map(_._2)))
      }
  }

  /**
   * Generic application of an externally-resolved id lookup (DrugBank id -> value), e.g. the
   * ChEMBL/PubChem/UMLS results produced by [[ApiEnrichment]] and cached to Parquet. The
   * lookup is small relative to the fact table, so it is broadcast.
   */
  def applyLookup(
      base: Dataset[DrugEntry],
      lookup: Dataset[(String, String)]
  )(set: (DrugEntry, String) => DrugEntry)(implicit spark: SparkSession): Dataset[DrugEntry] = {
    import spark.implicits._
    import org.apache.spark.sql.functions.broadcast
    val left = base.map(d => (d.drugbankId, d))
    left
      .joinWith(broadcast(lookup), left("_1") === lookup("_1"), "left")
      .map { case ((_, d), pair) =>
        Option(pair).map(_._2).fold(d)(v => set(d, v))
      }
  }
}
