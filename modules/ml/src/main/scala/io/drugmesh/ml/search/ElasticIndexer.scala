package io.drugmesh.ml.search

import io.drugmesh.core.DrugEntry
import io.drugmesh.pipeline.SearchConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.sql._

/**
 * The ELK search layer. Bulk-indexes the resolved mapping table into Elasticsearch via the
 * `elasticsearch-spark` connector and exposes the index mapping + query builders for fuzzy,
 * phonetic, entity-centric drug-name search — the "single source of truth" lookup pattern.
 */
object ElasticIndexer {

  /** Bulk-load the mapping table into Elasticsearch, keyed by DrugBank id. */
  def index(ds: Dataset[DrugEntry], cfg: SearchConf)(implicit spark: SparkSession): Unit = {
    val options = Map(
      "es.nodes"               -> cfg.esNodes,
      "es.port"                -> cfg.esPort.toString,
      "es.mapping.id"          -> "drugbankId",
      "es.write.operation"     -> "upsert",
      "es.nodes.wan.only"      -> "true"
    )
    ds.toDF().saveToEs(cfg.indexName, options)
  }

  /**
   * Index settings: a metaphone phonetic analyzer plus an edge-ngram analyzer for
   * type-ahead, and a `completion` suggester field. Apply via the ES `_settings`/`_mapping`
   * APIs before the first bulk load.
   */
  val indexSettings: String =
    """{
      |  "settings": {
      |    "analysis": {
      |      "filter": {
      |        "drug_metaphone": { "type": "phonetic", "encoder": "metaphone", "replace": false },
      |        "drug_edge_ngram": { "type": "edge_ngram", "min_gram": 2, "max_gram": 20 }
      |      },
      |      "analyzer": {
      |        "phonetic_name": { "tokenizer": "standard", "filter": ["lowercase", "drug_metaphone"] },
      |        "autocomplete":  { "tokenizer": "standard", "filter": ["lowercase", "drug_edge_ngram"] }
      |      }
      |    }
      |  },
      |  "mappings": {
      |    "properties": {
      |      "drugbankId": { "type": "keyword" },
      |      "name": {
      |        "type": "text",
      |        "fields": {
      |          "phonetic":   { "type": "text", "analyzer": "phonetic_name" },
      |          "autocomplete": { "type": "text", "analyzer": "autocomplete" },
      |          "raw":        { "type": "keyword" }
      |        }
      |      },
      |      "name_suggest": { "type": "completion" },
      |      "chembl_id": { "type": "keyword" },
      |      "pubchem_cid": { "type": "keyword" }
      |    }
      |  }
      |}""".stripMargin

  /** A fuzzy + phonetic multi-field search for a drug name, with field boosting. */
  def fuzzyQuery(term: String): String =
    s"""{
       |  "query": {
       |    "bool": {
       |      "should": [
       |        { "match": { "name":          { "query": "$term", "fuzziness": "AUTO", "boost": 3 } } },
       |        { "match": { "name.phonetic":  { "query": "$term", "boost": 2 } } },
       |        { "match": { "name.autocomplete": { "query": "$term", "boost": 1 } } }
       |      ],
       |      "minimum_should_match": 1
       |    }
       |  }
       |}""".stripMargin
}
