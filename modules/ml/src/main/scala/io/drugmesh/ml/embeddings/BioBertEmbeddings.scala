package io.drugmesh.ml.embeddings

import com.johnsnowlabs.nlp.{DocumentAssembler, EmbeddingsFinisher}
import com.johnsnowlabs.nlp.embeddings.BertSentenceEmbeddings
import io.drugmesh.core.DrugEntry
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * BioBERT / sentence-BERT embeddings of drug names and descriptions, via Spark NLP. Two uses:
 *   1. Semantic candidate generation for entity resolution — nearest-neighbor search over
 *      embeddings catches synonym pairs ("Acetaminophen" / "Paracetamol") that pure string
 *      similarity misses, feeding [[io.drugmesh.ml.entityres.Blocking]] a richer candidate set.
 *   2. Normalization to RxNorm/SNOMED concepts (Spark NLP's `SentenceEntityResolverModel`
 *      does this with sentence-BERT + a KD-tree nearest-neighbor search).
 *
 * Note: the licensed Spark NLP *for Healthcare* clinical resolvers require a John Snow Labs
 * license; the open `sent_small_bert`/`sbiobert_base_cased_mli` sentence models used here are
 * free and adequate for a portfolio.
 */
object BioBertEmbeddings {

  /** Build the embedding pipeline for a given pretrained sentence-BERT model. */
  def pipeline(model: String): Pipeline = {
    val document = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentenceEmbeddings = BertSentenceEmbeddings
      .pretrained(model, "en")
      .setInputCols("document")
      .setOutputCol("sentence_embeddings")
      .setCaseSensitive(false)

    val finisher = new EmbeddingsFinisher()
      .setInputCols("sentence_embeddings")
      .setOutputCols("embedding")
      .setOutputAsVector(true)
      .setCleanAnnotations(true)

    new Pipeline().setStages(Array(document, sentenceEmbeddings, finisher))
  }

  /** Embed drug names; returns `(drugbankId, embedding: Vector)` for kNN candidate generation. */
  def embedNames(
      ds: Dataset[DrugEntry],
      model: String = "sent_small_bert_L2_768"
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val input = ds.map(d => (d.drugbankId, d.name)).toDF("drugbankId", "text")
    pipeline(model)
      .fit(input)
      .transform(input)
      .select("drugbankId", "embedding")
  }
}
