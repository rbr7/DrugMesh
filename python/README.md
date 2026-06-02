# Snorkel labeling step

A small, deliberate Python boundary in an otherwise Scala/Spark project. Snorkel is
Python-only, so weak-supervision labeling runs as its own stage rather than being forced
into the JVM.

## Where it fits

```
Spark (Scala)                         Python (Snorkel)                 Spark (Scala)
EntityResolution.featureFrame  ──▶  snorkel_labeling.py        ──▶   EntityResolution.trainGbt
  candidate_pairs.parquet            labeled_pairs.parquet            GBT matcher (MLlib)
```

1. The Spark job writes candidate-pair comparison features to Parquet
   (`EntityResolution.featureFrame(...).write.parquet("out/candidate_pairs.parquet")`).
2. This script applies labeling functions (shared CAS / InChIKey / PubChem / ChEMBL, high
   Jaro-Winkler, low token overlap, ...), fits Snorkel's `LabelModel` to denoise them, and
   writes probabilistic labels.
3. The Spark MLlib `GBTClassifier` trains on those weak labels.

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python snorkel_labeling.py \
  --input  ../out/candidate_pairs.parquet \
  --output ../out/labeled_pairs.parquet
```

The labeling functions live in `snorkel_labeling.py` and intentionally mirror the
`Comparison.FeatureNames` column contract on the Scala side, so the two languages agree on
the feature schema.
