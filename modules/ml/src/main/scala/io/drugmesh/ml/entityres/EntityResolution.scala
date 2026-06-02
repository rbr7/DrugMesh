package io.drugmesh.ml.entityres

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** A scored candidate-pair decision with its auditable per-field breakdown. */
final case class MatchResult(
    idA: String,
    idB: String,
    posterior: Double,
    totalBits: Double,
    decision: String,
    contributions: Seq[FieldWeight]
)

/**
 * The headline extension: probabilistic entity resolution over the drug records — deciding
 * when two records refer to the same real-world drug despite missing ids and name variants.
 * This is the same problem statement as provider-directory reconciliation.
 *
 * Two interchangeable matchers are provided:
 *   - [[resolve]] uses the unsupervised [[FellegiSunter]] model; every decision is explained
 *     by per-field match weights (no labels needed).
 *   - [[trainGbt]] / [[scoreWithGbt]] train a discriminative gradient-boosted-tree matcher on
 *     the weak labels emitted by the Snorkel step, for when labeled signal is available.
 */
object EntityResolution {

  val Match    = "match"
  val Review   = "review"
  val NonMatch = "non-match"

  /** Unsupervised Fellegi-Sunter resolution. Returns matches and clerical-review pairs. */
  def resolve(
      records: Dataset[ResolutionRecord],
      model: FellegiSunter = FellegiSunter.default,
      matchThreshold: Double = 0.90,
      reviewLower: Double = 0.45,
      prefix: Int = 6
  )(implicit spark: SparkSession): Dataset[MatchResult] = {
    import spark.implicits._
    Blocking
      .candidatePairs(records, prefix)
      .map { case (a, b) =>
        val score = model.score(Comparison.features(a, b))
        val decision =
          if (score.posterior >= matchThreshold) Match
          else if (score.posterior >= reviewLower) Review
          else NonMatch
        MatchResult(a.id, b.id, score.posterior, score.totalBits, decision, score.contributions)
      }
      .filter(_.decision != NonMatch)
  }

  /** Materialize the comparison feature columns for the discriminative matcher. */
  def featureFrame(
      records: Dataset[ResolutionRecord],
      prefix: Int = 6
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    Blocking
      .candidatePairs(records, prefix)
      .map { case (a, b) =>
        val v = Comparison.toVector(Comparison.features(a, b))
        (a.id, b.id, v(0), v(1), v(2), v(3), v(4), v(5), v(6))
      }
      .toDF(("idA" +: "idB" +: Comparison.FeatureNames): _*)
  }

  private def assembler: VectorAssembler =
    new VectorAssembler().setInputCols(Comparison.FeatureNames.toArray).setOutputCol("features")

  /**
   * Train a GBT matcher on weakly-supervised labels (column `label`, produced by Snorkel's
   * LabelModel). Expects the comparison feature columns named per [[Comparison.FeatureNames]].
   */
  def trainGbt(labeled: DataFrame, maxIter: Int = 50): PipelineModel = {
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(maxIter)
      .setMaxDepth(5)
    new Pipeline().setStages(Array(assembler, gbt)).fit(labeled)
  }

  /** Apply a trained GBT matcher to candidate feature rows. */
  def scoreWithGbt(model: PipelineModel, features: DataFrame): DataFrame =
    model.transform(features)
}
