package io.drugmesh.ml.anomaly

import com.linkedin.relevance.isolationforest.IsolationForest
import io.drugmesh.core.{DrugEntry, Normalization}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Unsupervised data-quality anomaly detection — the "flag the dirty data" stage. Each row is
 * turned into a small numeric quality-signal vector (invalid CAS checksum, anomalous name
 * length/charset, suspiciously few resolved ids, malformed PubChem id) and scored with the
 * LinkedIn distributed **Isolation Forest** (a Spark/Scala-native Spark ML `Estimator`, so no
 * Python is needed). Isolation Forest is linear-time and label-free, which suits scoring the
 * whole table; each flagged row becomes a confidence-scored "data-quality alert".
 */
object AnomalyDetection {

  val FeatureCols: Array[String] =
    Array("cas_invalid", "name_len", "non_ascii", "filled_ids", "pubchem_malformed")

  /** Engineer per-row data-quality features. */
  def qualityFeatures(ds: Dataset[DrugEntry])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    ds.map { d =>
      val casInvalid       = if (d.casNum.exists(Normalization.isValidCas) || d.casNum.isEmpty) 0.0 else 1.0
      val nameLen          = d.name.length.toDouble
      val nonAscii         = d.name.count(_ > 127).toDouble
      val filledIds        = d.filledIdCount.toDouble
      val pubchemMalformed = if (d.pubchemCid.forall(_.forall(_.isDigit))) 0.0 else 1.0
      (d.drugbankId, casInvalid, nameLen, nonAscii, filledIds, pubchemMalformed)
    }.toDF(("drugbankId" +: FeatureCols): _*)
  }

  /**
   * Fit an isolation forest and return per-row outlier scores and a 0/1 anomaly flag.
   * `contamination` is the expected fraction of dirty rows; `maxSamples` is the subsample
   * size per tree.
   */
  def detect(
      ds: Dataset[DrugEntry],
      contamination: Double = 0.02,
      numEstimators: Int = 100,
      maxSamples: Int = 256
  )(implicit spark: SparkSession): DataFrame = {
    val assembled = new VectorAssembler()
      .setInputCols(FeatureCols)
      .setOutputCol("features")
      .transform(qualityFeatures(ds))

    val iforest = new IsolationForest()
      .setNumEstimators(numEstimators)
      .setMaxSamples(maxSamples)
      .setContamination(contamination)
      .setContaminationError(0.01 * contamination)
      .setFeaturesCol("features")
      .setPredictionCol("anomaly_flag")
      .setScoreCol("outlier_score")
      .setRandomSeed(42)

    iforest
      .fit(assembled)
      .transform(assembled)
      .select("drugbankId", "outlier_score", "anomaly_flag")
  }
}
