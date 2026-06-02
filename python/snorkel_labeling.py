#!/usr/bin/env python3
"""Weak-supervision labeling step for DrugMesh entity resolution.

There are no gold labels for "these two drug records are the same drug". Following the
Snorkel recipe, we express domain heuristics as *labeling functions* (LFs) over candidate
pairs, then let Snorkel's ``LabelModel`` denoise the noisy/conflicting LFs into a single
probabilistic label per pair -- without any ground truth. The resulting labeled Parquet is
consumed by the Scala/Spark ``EntityResolution.trainGbt`` matcher.

This is a deliberate polyglot boundary: Snorkel is Python-only, so it runs as a labeling
stage that reads the candidate-pair features emitted by the Spark job (``featureFrame``) and
writes labels back as Parquet. Nothing else in the pipeline depends on Python.

Usage:
    python snorkel_labeling.py --input  out/candidate_pairs.parquet \\
                               --output out/labeled_pairs.parquet
"""
from __future__ import annotations

import argparse
import logging

import pandas as pd
from snorkel.labeling import LabelingFunction, PandasLFApplier, LFAnalysis
from snorkel.labeling.model import LabelModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("drugmesh.snorkel")

# Label space.
ABSTAIN = -1
NON_MATCH = 0
MATCH = 1


# --- Labeling functions -----------------------------------------------------------------
# Each LF expresses one heuristic. LFs may be wrong or correlated; the LabelModel learns
# their accuracies and resolves conflicts. Feature columns mirror Comparison.FeatureNames
# on the Scala side.

def _lf(name):
    """Decorator-free LF factory so each heuristic stays a small pure function."""
    def wrap(f):
        return LabelingFunction(name=name, f=f)
    return wrap


@_lf("lf_shared_inchikey")
def lf_shared_inchikey(x):
    return MATCH if x.shared_inchikey == 1.0 else ABSTAIN


@_lf("lf_shared_cas")
def lf_shared_cas(x):
    return MATCH if x.shared_cas == 1.0 else ABSTAIN


@_lf("lf_shared_pubchem")
def lf_shared_pubchem(x):
    return MATCH if x.shared_pubchem == 1.0 else ABSTAIN


@_lf("lf_shared_chembl")
def lf_shared_chembl(x):
    return MATCH if x.shared_chembl == 1.0 else ABSTAIN


@_lf("lf_exact_name")
def lf_exact_name(x):
    return MATCH if x.name_jaro_winkler >= 0.999 else ABSTAIN


@_lf("lf_high_jaro")
def lf_high_jaro(x):
    if x.name_jaro_winkler >= 0.95:
        return MATCH
    if x.name_jaro_winkler < 0.70:
        return NON_MATCH
    return ABSTAIN


@_lf("lf_low_token_overlap_no_id")
def lf_low_token_overlap_no_id(x):
    no_id = (x.shared_cas == 0.0 and x.shared_inchikey == 0.0
             and x.shared_pubchem == 0.0 and x.shared_chembl == 0.0)
    return NON_MATCH if no_id and x.token_jaccard < 0.2 else ABSTAIN


LFS = [
    lf_shared_inchikey,
    lf_shared_cas,
    lf_shared_pubchem,
    lf_shared_chembl,
    lf_exact_name,
    lf_high_jaro,
    lf_low_token_overlap_no_id,
]


def label(df: pd.DataFrame, epochs: int = 500, seed: int = 42) -> pd.DataFrame:
    """Apply the LFs and fit the LabelModel; return df with probabilistic labels."""
    applier = PandasLFApplier(lfs=LFS)
    matrix = applier.apply(df=df)
    log.info("LF analysis:\n%s", LFAnalysis(L=matrix, lfs=LFS).lf_summary())

    model = LabelModel(cardinality=2, verbose=True)
    model.fit(L_train=matrix, n_epochs=epochs, seed=seed, log_freq=100)

    probs = model.predict_proba(L=matrix)            # P(non-match), P(match)
    preds = model.predict(L=matrix, tie_break_policy="abstain")

    out = df.copy()
    out["label"] = preds
    out["prob_match"] = probs[:, MATCH]
    # Drop rows the model still abstains on -- they carry no training signal.
    out = out[out["label"] != ABSTAIN].reset_index(drop=True)
    log.info("labeled %d / %d candidate pairs (%.1f%% retained)",
             len(out), len(df), 100.0 * len(out) / max(1, len(df)))
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Snorkel weak-supervision labeler for DrugMesh ER")
    parser.add_argument("--input", required=True, help="candidate-pair features Parquet (from Spark featureFrame)")
    parser.add_argument("--output", required=True, help="destination Parquet of labeled pairs")
    parser.add_argument("--epochs", type=int, default=500)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    log.info("loaded %d candidate pairs from %s", len(df), args.input)
    labeled = label(df, epochs=args.epochs)
    labeled.to_parquet(args.output, index=False)
    log.info("wrote %s", args.output)


if __name__ == "__main__":
    main()
