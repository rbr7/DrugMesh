package io.drugmesh.ml.entityres

import io.drugmesh.core.Normalization
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * Blocking keeps entity resolution from being O(n^2): instead of comparing every drug to
 * every other, records are bucketed by cheap keys (salt-stripped name prefix, InChIKey
 * skeleton) and only within-bucket pairs are emitted as candidates. A record can land in
 * several buckets, so candidate pairs are de-duplicated by id-pair afterward.
 */
object Blocking {

  /** Blocking keys for a record. Prefix-of-normalized-name and InChIKey skeleton. */
  def keysFor(r: ResolutionRecord, prefix: Int = 6): Seq[String] = {
    val nameKey  = "n:" + Normalization.blockingKey(r.entry.name, prefix)
    val inchiKey = r.inchiKey.flatMap(Normalization.inchiKeySkeleton).map("i:" + _)
    (Seq(nameKey) ++ inchiKey.toSeq).filter(_.length > 3)
  }

  /** Candidate pairs (idA < idB), de-duplicated across blocking keys. */
  def candidatePairs(
      records: Dataset[ResolutionRecord],
      prefix: Int = 6
  )(implicit spark: SparkSession): Dataset[(ResolutionRecord, ResolutionRecord)] = {
    import spark.implicits._

    val keyed: Dataset[(String, ResolutionRecord)] =
      records.flatMap(r => keysFor(r, prefix).map(k => (k, r)))

    val pairs: Dataset[(ResolutionRecord, ResolutionRecord)] =
      keyed.groupByKey(_._1).flatMapGroups { (_, iter) =>
        val recs = iter.map(_._2).toVector.distinctBy(_.id).sortBy(_.id)
        for {
          i <- recs.indices.iterator
          j <- (i + 1) until recs.length
        } yield (recs(i), recs(j))
      }

    // De-duplicate pairs that co-occur under multiple blocking keys.
    pairs
      .groupByKey { case (a, b) => (a.id, b.id) }
      .reduceGroups((x, _) => x)
      .map(_._2)
  }
}
