package io.drugmesh.ml.ner

import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.ner.NerConverter
import com.johnsnowlabs.nlp.annotators.ner.dl.NerDLModel
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Biomedical named-entity recognition over free text (DrugBank descriptions, PubMed
 * abstracts) to mine chemical/drug mentions that enrich the mapping table — the JD's "entity
 * extraction" item. Runs natively at Spark scale with the Spark NLP Scala API, so NER happens
 * in the same JVM as the ETL.
 *
 * Open models are used (`glove_100d` + a chemical/disease `NerDLModel`). The scispaCy
 * `en_ner_bc5cdr_md` model + UMLS `EntityLinker` is the easy Python on-ramp; the licensed
 * Spark NLP for Healthcare clinical NER models give the strongest accuracy but need a license.
 */
object BiomedicalNer {

  /** Build a chemical-NER pipeline producing finished entity spans + labels. */
  def pipeline(nerModel: String = "ner_chemicals"): Pipeline = {
    val document = new DocumentAssembler().setInputCol("text").setOutputCol("document")
    val sentence = new SentenceDetector().setInputCols("document").setOutputCol("sentence")
    val token    = new Tokenizer().setInputCols("sentence").setOutputCol("token")

    val embeddings = WordEmbeddingsModel
      .pretrained("glove_100d", "en")
      .setInputCols("sentence", "token")
      .setOutputCol("embeddings")

    val ner = NerDLModel
      .pretrained(nerModel, "en")
      .setInputCols("sentence", "token", "embeddings")
      .setOutputCol("ner")

    val converter = new NerConverter().setInputCols("sentence", "token", "ner").setOutputCol("entities")

    val finisher = new Finisher()
      .setInputCols("entities")
      .setOutputCols("chemicals")
      .setCleanAnnotations(true)
      .setOutputAsArray(true)

    new Pipeline().setStages(Array(document, sentence, token, embeddings, ner, converter, finisher))
  }

  /** Extract chemical mentions from a `(drugbankId, text)` dataset. */
  def extractChemicals(
      texts: Dataset[(String, String)],
      nerModel: String = "ner_chemicals"
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val input = texts.toDF("drugbankId", "text")
    pipeline(nerModel)
      .fit(input)
      .transform(input)
      .select("drugbankId", "chemicals")
  }
}
