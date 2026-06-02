# DrugMesh

**A Spark-scale, explainable data-quality & entity-resolution engine for pharmaceutical reference data.**

DrugMesh ingests ten messy public drug databases, resolves which records refer to the *same*
drug despite missing identifiers and name variants, flags inconsistent/bad records with
confidence scores, and exposes an auditable reason for every decision. It produces a unified
cross-reference table `drug-mappings.tsv`, mapping each drug across DrugBank, TTD, PubChem,
ChEMBL, ZINC, ChEBI, KEGG, BindingDB, UMLS and STITCH.

The engineering problem at its core is *reconciling identifiers for the same real-world entity
across dirty, inconsistent sources* : **is entity resolution**, the same problem that has to be
solved to clean provider directories, payer rosters, and other healthcare master data.

> DrugMesh is a Scala/Apache Spark re-implementation and significant extension of the
> Apache-2.0 `drug_id_mapping` toolkit from NCSR "Demokritos" (Aisopos et al.). See
> [Attribution](#attribution).

---

## Why Scala + Spark

- **Scales to billions of records.** The cross-mapping is join-heavy; expressing it as Spark
  `Dataset`/DataFrame joins (broadcast joins for small dimension tables, Catalyst + Adaptive
  Query Execution for skew) takes it from a single-JVM, API-bound script to a distributed job.
- **Scala is Spark's native language.** Target is **Scala 2.13 + Spark 3.5 LTS** (the build
  also validates against Spark 4.0 in CI; both are Scala 2.13 , Spark dropped 2.12, and Spark
  application code does not run on Scala 3).
- **Typed, testable transformations.** Each enrichment pass is a pure
  `Dataset[DrugEntry] => Dataset[DrugEntry]`, composed in a configured DAG instead of the
  original's 600-line imperative `main()` with hand-toggled passes.

## Architecture

```
            sources                    Spark ETL (typed DAG)              ML / text-mining               outputs
   ┌───────────────────────┐     ┌──────────────────────────┐   ┌──────────────────────────┐   ┌────────────────────┐
   │ DrugBank XML          │     │ ingest  → core domain     │   │ entity resolution (FS +  │   │ drug-mappings.tsv  │
   │ TTD / KEGG / STITCH    │ ──▶ │ enrichment passes:        │──▶│   GBT, weak-supervised)   │──▶│ (13-col contract)  │
   │ ChEMBL/UniChem (API)  │     │  base→ttd→chembl→pubchem  │   │ anomaly detection (iForest)│   │ ER match report    │
   │ PubChem PUG (API)     │     │  →kegg→umls→stitch→dgidb  │   │ NER + BioBERT embeddings  │   │ data-quality alerts │
   │ UMLS Metathesaurus    │     │ (broadcast joins, AQE)    │   │ Elasticsearch search       │   │ Elasticsearch index │
   └───────────────────────┘     └──────────────────────────┘   └──────────────────────────┘   └────────────────────┘
                                                                  explainability: per-field match weights + SHAP/LIME
```

## Module layout (sbt multi-project)

| Module     | Responsibility |
|------------|----------------|
| `core`     | Immutable `DrugEntry` domain model (`Option` fields, not the `"null"` sentinel), normalization, string similarity, CAS validation, provenance types. No Spark runtime dependency. |
| `ingest`   | Source parsers: DrugBank XML (`scala-xml`), TTD/KEGG/STITCH/DGIdb flat files (Spark readers). |
| `clients`  | Typed, effectful external API clients (ChEMBL/UniChem, PubChem PUG, UMLS ticket-auth, DGIdb) on **sttp client 4 + cats-effect 3**, with retry/backoff, rate limiting, and Parquet response caching. |
| `pipeline` | The enrichment DAG that replaces `CreateDrugMappings`, **PureConfig** (HOCON) configuration, and the legacy-TSV reader/writer. |
| `ml`       | The six ML / text-mining extensions (below), kept separate so the ETL compiles without the heavy ML deps. |
| `app`      | `decline` CLI entrypoint. |
| `python/`  | The single Python boundary: a Snorkel weak-supervision labeling step. |

## The six ML / text-mining extensions

1. **Biomedical NER** (`ml.ner`) — mine chemical/drug mentions from DrugBank descriptions and
   abstracts with Spark NLP, natively at Spark scale.
2. **Entity resolution** (`ml.entityres`) — the headline capability. Blocking (normalized-name
   prefix + InChIKey skeleton) → comparison features (Jaro-Winkler, Levenshtein, token Jaccard,
   shared CAS/InChIKey/CID) → a **Fellegi-Sunter** probabilistic matcher whose per-field match
   weights *are* the explanation, plus an optional MLlib **GBT** matcher trained on weak labels.
3. **Weak supervision** (`python/snorkel_labeling.py`) — Snorkel labeling functions + `LabelModel`
   turn heuristics into probabilistic labels with no ground truth, emitted as Parquet for the GBT.
4. **Anomaly detection** (`ml.anomaly`) — LinkedIn's distributed **Isolation Forest** (Spark/Scala
   native) scores each row on data-quality signals (invalid CAS checksum, malformed ids, charset
   outliers) and flags "dirty data" with confidence.
5. **Search** (`ml.search`) — bulk-index into **Elasticsearch** with fuzzy (`fuzziness: AUTO`),
   phonetic (metaphone), and autocomplete analyzers for entity-centric drug-name lookup.
6. **Embeddings** (`ml.embeddings`) — BioBERT / sentence-BERT embeddings (Spark NLP) for semantic
   candidate generation in ER and RxNorm/SNOMED-style normalization.

## Explainability

Explainability is a first-class output, not an afterthought. The Fellegi-Sunter matcher is
additive in log-space, so every decision decomposes into per-field bit contributions:

```
DB00945 ~ DB13746: MATCH (posterior 0.999, total +33.1 bits) —
  CAS number agreement: +16.6 bits; InChIKey skeleton agreement: +12.9 bits;
  name (Jaro-Winkler) agreement: +5.4 bits; name tokens (Jaccard) agreement: +4.8 bits
```

For the discriminative GBT matcher, attach **SHAP** (global, consistent attributions) and
**LIME** (local, per-prediction). Every score maps to a human-readable reason string —
the record-level recommendations a non-technical reviewer can audit.

## Quick start

```bash
# Build, format-check, test (with coverage)
sbt scalafmtCheckAll compile test

# Run a subcommand (Spark is on the local run classpath via the `app` module)
sbt "app/run build-mappings   --config conf/pipeline.conf"
sbt "app/run resolve          --threshold 0.9"
sbt "app/run detect-anomalies"
sbt "app/run index"
```

Configuration (sources, enrichment order, thresholds, API endpoints) lives in
[`conf/pipeline.conf`](conf/pipeline.conf). Credentialed/rate-limited APIs (UMLS, ChEMBL,
PubChem) cache responses to Parquet, so reruns and CI never depend on live calls. The UMLS API
key is read from the `UMLS_API_KEY` environment variable and must never be committed.

## Output contract

The emitted `drug-mappings.tsv` is byte-compatible with the original 13-column format
(`"null"` for absent ids, comma-separated UMLS CUIs), so the rewrite can be validated by diff
against the reference artifact:

```
drugbankId  name  ttd_id  pubchem_cid  cas_num  chembl_id  zinc_id  chebi_id  kegg_cid  kegg_id  bindingDB_id  UMLS_cuis  stitch_id
DB13088     AZD-0424  D0QG8F  9893171  692054-06-1  CHEMBL3545177  null  null  null  null  null  C4519307  null
```

## Testing & CI

- Pure transforms unit-tested with **MUnit**; Spark `Dataset` tests run against a local
  `SparkSession`; property tests with **ScalaCheck**.
- `scalafmt` + `scalafix` formatting/linting, `sbt-scoverage` coverage.
- GitHub Actions matrix builds against **Spark 3.5.3 and 4.0.1** and lints the Python step.

## Attribution

DrugMesh is an independent Scala/Spark re-implementation and extension of the Apache-2.0
`drug_id_mapping` toolkit by Fotis Aisopos / NCSR "Demokritos"
([repo](https://github.com/iit-Demokritos/drug_id_mapping)). It reuses the public column
contract of the original dataset. If you use the resulting drug-mappings dataset, please cite:

> Aisopos, F., Paliouras, G. *Comparing methods for drug–gene interaction prediction on the
> biomedical literature knowledge graph: performance versus explainability.* BMC Bioinformatics
> 24, 272 (2023). https://doi.org/10.1186/s12859-023-05373-2

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
