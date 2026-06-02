package io.drugmesh.pipeline

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._
import io.drugmesh.clients.{ChemblClient, PubChemClient, UmlsClient}
import io.drugmesh.core.DrugEntry
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.concurrent.duration._

/**
 * Bridges Spark and the effectful API clients. The pattern, applied per source:
 *   1. On the driver, collect the *distinct* keys (DrugBank id + name) of entries still
 *      missing the target id — never call an API once per row.
 *   2. Resolve them with cats-effect `parTraverseN` (bounded concurrency + per-call throttle).
 *   3. Persist the resolved lookup to Parquet so reruns never re-hit the rate-limited API.
 *   4. Return the lookup as a `Dataset[(drugbankId, value)]` for a broadcast join back.
 *
 * Caching responses to Parquet (step 3) is the key scalability/robustness move called out in
 * the design: CI and reruns read the cache instead of the network.
 */
object ApiEnrichment {

  private def cached(spark: SparkSession, path: String): Option[Dataset[(String, String)]] = {
    import spark.implicits._
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(new Path(path))) Some(spark.read.parquet(path).as[(String, String)]) else None
  }

  private def persist(ds: Dataset[(String, String)], path: String): Dataset[(String, String)] = {
    ds.write.mode("overwrite").parquet(path)
    ds
  }

  /** Run a bounded-concurrency, throttled resolution over distinct keys. */
  private def resolve[K, V](
      keys: Seq[K],
      concurrency: Int,
      minInterval: FiniteDuration
  )(f: K => IO[Option[V]]): List[V] =
    keys.toList
      .parTraverseN(concurrency)(k => f(k) <* IO.sleep(minInterval))
      .map(_.flatten)
      .unsafeRunSync()

  /** Resolve PubChem CIDs by name for entries missing one. */
  def resolvePubChemByName(
      base: Dataset[DrugEntry],
      client: PubChemClient,
      cacheDir: String,
      concurrency: Int = 4,
      minInterval: FiniteDuration = 200.millis
  )(implicit spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._
    val path = s"$cacheDir/pubchem-cid"
    cached(spark, path).getOrElse {
      val keys = base.filter(_.pubchemCid.isEmpty).map(d => (d.drugbankId, d.name)).distinct().collect().toSeq
      val out = resolve(keys, concurrency, minInterval) { case (db, name) =>
        client.cidForName(name).map(_.toOption.flatten.map(cid => (db, cid)))
      }
      persist(spark.createDataset(out), path)
    }
  }

  /** Resolve ChEMBL ids via UniChem by DrugBank id for entries missing one. */
  def resolveChemblByDrugBank(
      base: Dataset[DrugEntry],
      client: ChemblClient,
      cacheDir: String,
      concurrency: Int = 4,
      minInterval: FiniteDuration = 200.millis
  )(implicit spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._
    val path = s"$cacheDir/chembl-id"
    cached(spark, path).getOrElse {
      val keys = base.filter(_.chemblId.isEmpty).map(_.drugbankId).distinct().collect().toSeq
      val out = resolve(keys, concurrency, minInterval) { db =>
        client.chemblForDrugBank(db).map(_.toOption.flatten.map(ch => (db, ch)))
      }
      persist(spark.createDataset(out), path)
    }
  }

  /** Resolve UMLS CUIs by name; multiple CUIs are comma-joined into the lookup value. */
  def resolveUmlsByName(
      base: Dataset[DrugEntry],
      client: UmlsClient,
      cacheDir: String,
      concurrency: Int = 2,
      minInterval: FiniteDuration = 300.millis
  )(implicit spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._
    val path = s"$cacheDir/umls-cuis"
    cached(spark, path).getOrElse {
      val keys = base.filter(_.umlsCuis.isEmpty).map(d => (d.drugbankId, d.name)).distinct().collect().toSeq
      val out = resolve(keys, concurrency, minInterval) { case (db, name) =>
        client.cuisForName(name).map(_.toOption.filter(_.nonEmpty).map(cuis => (db, cuis.mkString(","))))
      }
      persist(spark.createDataset(out), path)
    }
  }

  // Field setters used with Enrichment.applyLookup ------------------------------------------
  val setPubChem: (DrugEntry, String) => DrugEntry = (d, v) => d.copy(pubchemCid = d.pubchemCid.orElse(Some(v)))
  val setChembl: (DrugEntry, String) => DrugEntry  = (d, v) => d.copy(chemblId = d.chemblId.orElse(Some(v)))
  val setUmls: (DrugEntry, String) => DrugEntry =
    (d, v) => if (d.umlsCuis.nonEmpty) d else d.copy(umlsCuis = v.split(",").toIndexedSeq.filter(_.nonEmpty))
}
