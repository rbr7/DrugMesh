# Project status

An honest accounting of what is implemented, what is scaffolded, and what has actually been
verified which is the companion to the architecture and claims in the README.

## Verification note (read this first)

The project has **not** been run through `sbt scalafmtCheckAll compile test` in its authoring
environment, because no Scala toolchain (sbt/scalac) was available there. The code is written
and statically reviewed; it has **not been compiled**. What *was* checked mechanically:

- every Scala file's `package` matches its directory path (37/37);
- brackets/parens/braces balance in every Scala file (string- and comment-aware);
- the Python labeling step parses (`ast.parse`).

**CI is the source of truth.** The README badge is a live GitHub Actions status badge: it will
show no-status/red until the workflow runs green on a push. Treat anything below marked
"Implemented" as "implemented and statically reviewed", not "proven to compile", until CI is
green.

## Module status

| Component | Status | Automated test | Runs without external services? |
|---|---|---|---|
| `core` : `DrugEntry`, `Normalization`, `SourceDb`, `Provenance` | Implemented | `NormalizationSpec` (pure) | Yes |
| `ingest` : DrugBank XML parser | Implemented | `DrugBankXmlParserSpec` (pure) | Yes |
| `ingest` : TTD / KEGG / STITCH / DGIdb readers | Implemented | KEGG block-parse tested | Needs Spark + the source files |
| `clients` : ChEMBL/UniChem, PubChem, UMLS, DGIdb (sttp4 + cats-effect) | Implemented | `CodecsSpec`, `HttpSupportSpec` (pure) | Live calls need network; UMLS needs a licensed API key |
| `pipeline` : enrichment DAG, PureConfig, TSV writer | Implemented | `EnrichmentSpec` (local Spark) | Needs Spark |
| `ml.entityres` : blocking, comparison, Fellegi-Sunter | Implemented | `ComparisonSpec` (pure) | FS scorer: yes; blocking/featureFrame: needs Spark |
| `ml.explain` : match explanations | Implemented | covered by `ComparisonSpec` | Yes |
| `ml.entityres` : GBT discriminative matcher | Implemented (wiring) | : | Needs Spark MLlib + Snorkel labels |
| `ml.anomaly` : isolation-forest data-quality scoring | Implemented (feature engineering real) | : | Feature step: Spark; iForest fit/transform needs the LinkedIn lib |
| `ml.ner` : biomedical NER | **Scaffold** | : | Needs Spark NLP + a pretrained model download |
| `ml.embeddings` : BioBERT / sentence-BERT | **Scaffold** | : | Needs Spark NLP + a sentence-BERT model |
| `ml.search` : Elasticsearch indexing/search | **Scaffold** | : | Needs a running Elasticsearch + es-spark connector |
| `python/` : Snorkel weak-supervision labeling | Implemented | `ruff` lint in CI | Needs a Python env with snorkel |

**"Scaffold"** = real, idiomatic wiring against the library's documented API, but it cannot run
in CI without external models/services and has no automated test yet.

## Tests that run anywhere (no Spark, no network)

`core/NormalizationSpec`, `ingest/DrugBankXmlParserSpec`, `clients/CodecsSpec`,
`clients/HttpSupportSpec`, `ml/ComparisonSpec`.

## Tests that need a Spark runtime (run in CI)

`pipeline/EnrichmentSpec`.

## Known gaps / next steps

1. Compile on a machine with sbt; fix whatever the first CI run surfaces, then confirm the badge
   is green and replace the `OWNER` placeholder in the README badge URL.
2. Add a small DrugBank XML sample under `data/sources/` so `app/run build-mappings` runs
   end-to-end out of the box.
3. The `sbt-scalafix` plugin is configured but no scalafix rules are enabled yet; the
   `sbt-scoverage` plugin is present but not wired into the minimal CI.
4. `ScalaCheck` is on the test classpath but no property tests are written yet.
5. NER / embeddings / search: add integration tests behind a flag (testcontainers for ES, a tiny
   model for Spark NLP), or treat them as optional modules.
6. The Fellegi-Sunter `m`/`u` values are sensible priors, not yet EM-trained on the
   Snorkel-labeled set.
