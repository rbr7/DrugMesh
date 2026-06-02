# Architecture & design decisions

This document records the *why* behind DrugMesh's structure — the trade-offs a reviewer would
otherwise have to reverse-engineer from the code.

## 1. From imperative script to typed DAG

The original `CreateDrugMappings` was a single ~600-line class whose `main()` enabled
enrichment passes by commenting lines in and out, mutating shared `HashMap`s as it went.

DrugMesh models each pass as a pure, typed transformation:

```scala
type Pass = Dataset[DrugEntry] => Dataset[DrugEntry]
```

and folds an **ordered, configured** list of passes (`drugmesh.enrichment.passes` in HOCON)
over a seed dataset. Consequences:

- Passes are individually unit-testable (`EnrichmentSpec`).
- Reordering/enabling a pass is a config edit, not a recompile.
- Each pass fills only *missing* fields (existing values win), so the merge is monotonic and
  order-tolerant where the data allows.

## 2. `Option`, not `"null"`

The legacy schema used the string `"null"` as an absent-value sentinel. DrugMesh uses
`Option[String]` end-to-end and re-applies the `"null"` token only at the very edge, in
`DrugEntry.toTsvRow`, so the output stays byte-compatible while the in-memory model is
type-safe. `umlsCuis` is a real `Seq[String]` instead of a comma-joined string.

## 3. Effects & error handling (cats-effect 3)

API access is effectful and failure-prone. The `clients` module:

- represents recoverable failures as a data `ApiError` ADT, not exceptions, so one bad API
  response cannot abort a batch;
- wraps calls in exponential backoff + full jitter, retrying only *transient* errors (network,
  5xx, 429) and failing fast on decode/auth/4xx;
- bounds concurrency and request rate (`parTraverseN` + per-call throttle);
- decodes bodies as strings and parses with circe explicitly, keeping error mapping stable
  across sttp minor versions.

## 4. Spark scalability story

The cross-mapping is join-heavy, which is where the big-data competencies live:

- **Broadcast joins** for small dimension tables (the API-resolved id lookups) against the
  large fact table (`Enrichment.applyLookup` uses `broadcast(...)`).
- **Adaptive Query Execution** is enabled (coalesce shuffle partitions, skew-join handling) —
  see `SparkSupport.session`.
- **Blocking before comparison** keeps entity resolution out of O(n²): records bucket by
  salt-stripped name prefix and InChIKey skeleton, and only within-bucket pairs are compared.
- **API response caching to Parquet** means reruns and CI read the cache instead of hitting
  rate-limited endpoints.
- Talking points to be ready for: narrow vs wide transformations; why `reduceByKey` beats
  `groupByKey`; salting skewed name keys; `repartition` vs `coalesce`; predicate pushdown on
  Parquet; Delta `OPTIMIZE`/`VACUUM` for the small-files problem.

## 5. Entity resolution: two interchangeable matchers

- **Fellegi-Sunter (default, unsupervised).** Per-field `m`/`u` probabilities give a log-space
  additive match weight; the per-field bit contributions *are* the explanation. No labels
  needed. In production the `m`/`u` values are learned with EM (Splink-style).
- **GBT (discriminative, weakly-supervised).** When labels exist, an MLlib `GBTClassifier`
  trains on the **Snorkel**-produced weak labels. SHAP/LIME provide its explanations.

Both consume the same `Comparison` feature contract, shared with the Python labeling step.

## 6. The one deliberate polyglot boundary

Snorkel is Python-only. Rather than contort it into the JVM, weak-supervision labeling is its
own stage: Spark writes candidate-pair features to Parquet → `python/snorkel_labeling.py`
writes probabilistic labels → Spark MLlib trains on them. The feature schema is the contract
both sides agree on. Nothing else depends on Python.

## 7. Explainability as a first-class output

Regulated healthcare data work needs auditable reasoning, so every score maps to a
human-readable reason string (`ml.explain.MatchExplanation`). The accuracy-vs-interpretability
trade-off is explicit: an auditable, slightly-less-accurate additive model is often preferable
to an opaque one for payer/clinical trust.

## 8. Module dependency graph

```
core  ◀── ingest ◀──┐
  ▲                  │
  └──── clients ◀────┼── pipeline ◀── ml ◀── app
                     └───────────────┘
```

`core` has no Spark *usage* (encoders are derived at use sites via `import spark.implicits._`),
keeping the domain model portable and free of implicit-encoder ambiguity.

## Caveats / honesty notes

- Spark NLP's clinical entity resolvers (`sbiobertresolve_*`) are a **commercial** John Snow
  Labs product; the open sentence-BERT / NER models used here are free and adequate for a
  portfolio.
- External APIs (PubChem PUG, UMLS, ChEMBL) are rate-limited and (for UMLS) credentialed;
  responses are cached and CI does not depend on live calls.
- The dependency versions in `build.sbt` are pinned to a coherent Spark-3.5/Scala-2.13 set;
  verify against your resolver before a production bump.
