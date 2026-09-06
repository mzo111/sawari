"""Train the XGBoost ETA model and score it against the baseline on the
identical holdout. Writes one row to model_registry unless --dry-run.

  python -m ml.train [--split temporal|group] [--drop-vessel-attrs] [--dry-run]
  python -m ml.train --diagnostics     # 2x2 grid on one snapshot, no writes

Defaults only. The baseline is recomputed on the same in-memory split so
the comparison is like-for-like regardless of how the dataset has grown.
Uses the native xgb.train API: XGBRegressor needs scikit-learn, which is
not in the stack.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import numpy as np
import xgboost as xgb

from .baseline import SOG_FLOOR_KN, group_split, mae, temporal_split
from .dataset import Row, load_rows
from .eval import baseline_predict, report
from .features import FEATURES, VESSEL_ATTRS, featurize
from .portcalls_job import MAX_KMH

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://sawari:sawari@localhost:5432/sawari"
)
REPO_ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = REPO_ROOT / "ml" / "artifacts"
MODEL_NAME = "xgb_eta"

# XGBoost's documented defaults, written out because xgb.train has no
# n_estimators of its own; 100 rounds is XGBRegressor's default. seed=0 is
# reproducibility, not tuning.
XGB_PARAMS = {"objective": "reg:squarederror", "eta": 0.3, "max_depth": 6, "seed": 0}
NUM_ROUNDS = 100

SPLITS = {"temporal": temporal_split, "group": group_split}

INSERT_RUN = """
INSERT INTO model_registry
    (name, version, train_rows, holdout_start, baseline_mae_hours, model_mae_hours,
     params, artifact_path, is_active)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, false)
RETURNING id
"""


def select(rows: list[Row], names: list[str]) -> np.ndarray:
    idx = [FEATURES.index(n) for n in names]
    return np.array([featurize(r) for r in rows], dtype=float)[:, idx]


def dmatrix(rows: list[Row], names: list[str], with_labels: bool = False) -> xgb.DMatrix:
    y = np.array([r.hours_to_arrival for r in rows], dtype=float) if with_labels else None
    return xgb.DMatrix(select(rows, names), label=y, feature_names=names, missing=np.nan)


def importances(booster: xgb.Booster, names: list[str]) -> list[tuple[str, float, int]]:
    gain = booster.get_score(importance_type="gain")
    weight = booster.get_score(importance_type="weight")
    total = sum(gain.values()) or 1.0
    table = [(n, gain.get(n, 0.0) / total, int(weight.get(n, 0))) for n in names]
    return sorted(table, key=lambda t: -t[1])


def vessels(rows: list[Row]) -> set[int]:
    return {r.mmsi for r in rows}


def calls(rows: list[Row]) -> set[int]:
    return {r.call_id for r in rows}


def print_overlap(rows: list[Row], holdout_frac: float) -> None:
    train, holdout, cutoff = temporal_split(rows, holdout_frac)
    both = vessels(train) & vessels(holdout)
    leaky_calls = {r.call_id for r in holdout if r.mmsi in vessels(train)}
    print(f"overlap under the temporal split (cutoff {cutoff.isoformat()}):")
    print(f"   distinct MMSIs in dataset:            {len(vessels(rows))}  (calls={len(calls(rows))})")
    print(f"   train:   calls={len(calls(train)):3d} vessels={len(vessels(train)):3d}")
    print(f"   holdout: calls={len(calls(holdout)):3d} vessels={len(vessels(holdout)):3d}")
    print(f"   MMSIs on both sides:                  {len(both)}")
    print(f"   holdout calls whose vessel is in train: {len(leaky_calls)} of {len(calls(holdout))}")


async def run_config(
    rows: list[Row],
    split: str,
    drop_vessel_attrs: bool,
    holdout_frac: float,
    *,
    write: bool,
    conn: asyncpg.Connection,
) -> dict:
    names = [f for f in FEATURES if f not in VESSEL_ATTRS] if drop_vessel_attrs else list(FEATURES)
    feature_label = f"no-vessel-attrs({len(names)})" if drop_vessel_attrs else f"all({len(names)})"
    train, holdout, cutoff = SPLITS[split](rows, holdout_frac)
    assert len(train) + len(holdout) <= len(rows)
    assert all(r.arrival_at < cutoff for r in train)
    assert all(r.arrival_at >= cutoff for r in holdout)
    assert not (calls(train) & calls(holdout))
    if split == "group":
        assert not (vessels(train) & vessels(holdout))

    booster = xgb.train(XGB_PARAMS, dmatrix(train, names, with_labels=True), num_boost_round=NUM_ROUNDS)

    def model_predict(rs: list[Row]) -> list[float]:
        return [float(v) for v in booster.predict(dmatrix(rs, names))]

    print(f"\n######## split={split} features={feature_label}")
    print(f"dataset: rows={len(rows)} calls={len(calls(rows))} gate=MAX_KMH {MAX_KMH:.0f} "
          f"baseline_floor={SOG_FLOOR_KN:.0f} kn holdout_frac={holdout_frac} xgboost={xgb.__version__}")
    print(f"cutoff on arrival_at: {cutoff.isoformat()}")
    print(f"train:   rows={len(train)} calls={len(calls(train))} vessels={len(vessels(train))}")
    print(f"holdout: rows={len(holdout)} calls={len(calls(holdout))} vessels={len(vessels(holdout))}")
    print(f"model: xgb.train {XGB_PARAMS} num_boost_round={NUM_ROUNDS}")

    X_train = select(train, names)
    nan_share = {n: float(np.isnan(X_train[:, i]).mean()) for i, n in enumerate(names)}
    print("NaN share per feature, train: " + ", ".join(f"{n}={s * 100:.1f}%" for n, s in nan_share.items() if s))

    y_hold = [r.hours_to_arrival for r in holdout]
    base_mae = mae(list(zip(baseline_predict(holdout), y_hold, strict=True)))
    model_mae = mae(list(zip(model_predict(holdout), y_hold, strict=True)))
    improvement = (base_mae - model_mae) / base_mae * 100.0

    print(f"*** holdout rows={len(holdout)} calls={len(calls(holdout))} | "
          f"baseline MAE={base_mae:.3f} h  model MAE={model_mae:.3f} h  "
          f"improvement={improvement:+.1f}%  (train calls={len(calls(train))})")

    report("holdout - baseline", holdout, baseline_predict)
    report("holdout - xgboost", holdout, model_predict)
    report("train - xgboost, in-sample (reference only)", train, model_predict)

    print("\nfeature importances (gain share, split count):")
    for name, share, splits in importances(booster, names):
        print(f"   {name:>20}  gain={share * 100:5.1f}%  splits={splits}")

    summary = {
        "split": split, "features": feature_label,
        "train_calls": len(calls(train)), "train_vessels": len(vessels(train)), "train_rows": len(train),
        "holdout_calls": len(calls(holdout)), "holdout_vessels": len(vessels(holdout)),
        "holdout_rows": len(holdout),
        "baseline_mae": base_mae, "model_mae": model_mae, "improvement_pct": improvement,
    }
    if not write:
        return summary

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    ARTIFACTS.mkdir(exist_ok=True)
    artifact = ARTIFACTS / f"{MODEL_NAME}_{version}.json"
    booster.save_model(artifact)
    params = {
        "features": names, "split": split, "xgboost": xgb.__version__,
        "xgb_params": {**XGB_PARAMS, "num_boost_round": NUM_ROUNDS},
        "holdout_frac": holdout_frac, "cutoff": cutoff.isoformat(),
        "gate_max_kmh": MAX_KMH, "baseline_sog_floor_kn": SOG_FLOOR_KN,
        "nan_share_train": nan_share, **summary,
    }
    run_id = await conn.fetchval(
        INSERT_RUN, MODEL_NAME, version, len(train), cutoff, base_mae, model_mae,
        json.dumps(params), str(artifact.relative_to(REPO_ROOT)),
    )
    print(f"\nmodel_registry: id={run_id} name={MODEL_NAME} version={version} "
          f"artifact={artifact.relative_to(REPO_ROOT)}")
    return summary


def print_summary(results: list[dict]) -> None:
    print("\n" + "=" * 100)
    print(f"{'split':<9}{'features':<22}{'train c/v/rows':<18}{'holdout c/v/rows':<20}"
          f"{'baseline':>10}{'model':>10}{'improve':>10}")
    for s in results:
        print(f"{s['split']:<9}{s['features']:<22}"
              f"{s['train_calls']}/{s['train_vessels']}/{s['train_rows']:<10}"
              f"{s['holdout_calls']}/{s['holdout_vessels']}/{s['holdout_rows']:<12}"
              f"{s['baseline_mae']:>10.3f}{s['model_mae']:>10.3f}{s['improvement_pct']:>+9.1f}%")
    print("=" * 100)


async def main(args: argparse.Namespace) -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await load_rows(conn)
        if args.diagnostics:
            print_overlap(rows, args.holdout_frac)
            results = [
                await run_config(rows, split, drop, args.holdout_frac, write=False, conn=conn)
                for split in ("temporal", "group")
                for drop in (False, True)
            ]
            print_summary(results)
        else:
            await run_config(rows, args.split, args.drop_vessel_attrs, args.holdout_frac,
                             write=not args.dry_run, conn=conn)
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--holdout-frac", type=float, default=0.2)
    p.add_argument("--split", choices=sorted(SPLITS), default="temporal")
    p.add_argument("--drop-vessel-attrs", action="store_true")
    p.add_argument("--dry-run", action="store_true", help="no artifact, no registry row")
    p.add_argument("--diagnostics", action="store_true",
                   help="2x2 grid (split x features) on one snapshot; implies --dry-run")
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
