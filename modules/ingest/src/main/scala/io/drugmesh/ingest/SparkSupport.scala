package io.drugmesh.ingest

import org.apache.spark.sql.SparkSession

/** Small factory + helpers for obtaining a configured local or cluster `SparkSession`. */
object SparkSupport {

  /**
   * Build a session with Adaptive Query Execution on (runtime partition coalescing and
   * skew-join handling — relevant once the cross-mapping joins run at MCheck scale).
   */
  def session(
      appName: String = "drugmesh",
      master: Option[String] = Some("local[*]"),
      shufflePartitions: Int = 64,
      adaptive: Boolean = true
  ): SparkSession = {
    val base = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.shuffle.partitions", shufflePartitions)
      .config("spark.sql.adaptive.enabled", adaptive)
      .config("spark.sql.adaptive.coalescePartitions.enabled", adaptive)
      .config("spark.sql.adaptive.skewJoin.enabled", adaptive)
    master.fold(base)(m => base.master(m)).getOrCreate()
  }
}
