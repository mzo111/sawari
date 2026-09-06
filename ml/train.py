"""Train the XGBoost ETA model and score it against the baseline on the
identical temporal holdout. Writes one row to model_registry.

  python -m ml.train [--holdout-frac 0.2]

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

from .baseline import SOG_FLOOR_KN, mae, temporal_split
from .dataset import Row, load_rows
from .eval import baseline_predict, report
from .features import FEATURES, featurize
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

INSERT_RUN = """
INSERT INTO model_registry
    (name, version, train_rows, holdout_start, baseline_mae_hours, model_mae_hours,
     params, artifact_path, is_active)
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, false)
RETURNING id
"""


def dmatrix(rows: list[Row], with_labels: bool = False) -> xgb.DMatrix:
    X = np.array([featurize(r) for r in rows], dtype=float)
    y = np.array([r.hours_to_arrival for r in rows], dtype=float) if with_labels else None
    return xgb.DMatrix(X, label=y, feature_names=FEATURES, missing=np.nan)


def importances(booster: xgb.Booster) -> list[tuple[str, float, int]]:
    gain = booster.get_score(importance_type="gain")
    weight = booster.get_score(importance_type="weight")
    total = sum(gain.values()) or 1.0
    table = [(name, gain.get(name, 0.0) / total, int(weight.get(name, 0))) for name in FEATURES]
    return sorted(table, key=lambda t: -t[1])


async def main(holdout_frac: float) -> None:
    conn = await asyncpg.connect(DATABASE_URL)
    try:
        rows = await load_rows(conn)
        train, holdout, cutoff = temporal_split(rows, holdout_frac)
        assert len(train) + len(holdout) == len(rows)
        assert all(r.arrival_at < cutoff for r in train)
        assert all(r.arrival_at >= cutoff for r in holdout)
        assert not ({r.call_id for r in train} & {r.call_id for r in holdout})

        train_calls = len({r.call_id for r in train})
        holdout_calls = len({r.call_id for r in holdout})

        dtrain = dmatrix(train, with_labels=True)
        booster = xgb.train(XGB_PARAMS, dtrain, num_boost_round=NUM_ROUNDS)

        def model_predict(rs: list[Row]) -> list[float]:
            return [float(v) for v in booster.predict(dmatrix(rs))]

        print(f"dataset: rows={len(rows)} calls={len({r.call_id for r in rows})} "
              f"gate=MAX_KMH {MAX_KMH:.0f} baseline_floor={SOG_FLOOR_KN:.0f} kn "
              f"holdout_frac={holdout_frac} xgboost={xgb.__version__}")
        print(f"temporal cutoff on arrival_at: {cutoff.isoformat()}")
        print(f"train:   rows={len(train)} calls={train_calls}")
        print(f"holdout: rows={len(holdout)} calls={holdout_calls}")
        print(f"model: xgb.train {XGB_PARAMS} num_boost_round={NUM_ROUNDS} "
              f"(XGBoost defaults; native API because XGBRegressor requires scikit-learn)")

        X_train = np.array([featurize(r) for r in train], dtype=float)
        nan_share = {n: float(np.isnan(X_train[:, i]).mean()) for i, n in enumerate(FEATURES)}
        print("NaN share per feature, train:")
        for name, share in nan_share.items():
            print(f"   {name:>20}  {share * 100:5.1f}%")

        y_hold = [r.hours_to_arrival for r in holdout]
        base_mae = mae(list(zip(baseline_predict(holdout), y_hold, strict=True)))
        model_mae = mae(list(zip(model_predict(holdout), y_hold, strict=True)))
        improvement = (base_mae - model_mae) / base_mae * 100.0

        print(f"\n*** holdout rows={len(holdout)} calls={holdout_calls} | "
              f"baseline MAE={base_mae:.3f} h  model MAE={model_mae:.3f} h  "
              f"improvement={improvement:+.1f}%  (train calls={train_calls})")

        report("holdout - baseline", holdout, baseline_predict)
        report("holdout - xgboost", holdout, model_predict)
        report("train - xgboost, in-sample (reference only)", train, model_predict)

        print("\nfeature importances (gain share, split count):")
        for name, share, splits in importances(booster):
            print(f"   {name:>20}  gain={share * 100:5.1f}%  splits={splits}")

        version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        ARTIFACTS.mkdir(exist_ok=True)
        artifact = ARTIFACTS / f"{MODEL_NAME}_{version}.json"
        booster.save_model(artifact)

        params = {
            "features": FEATURES,
            "xgboost": xgb.__version__,
            "xgb_params": {**XGB_PARAMS, "num_boost_round": NUM_ROUNDS},
            "holdout_frac": holdout_frac,
            "cutoff": cutoff.isoformat(),
            "train_calls": train_calls,
            "holdout_calls": holdout_calls,
            "holdout_rows": len(holdout),
            "gate_max_kmh": MAX_KMH,
            "baseline_sog_floor_kn": SOG_FLOOR_KN,
            "nan_share_train": nan_share,
            "improvement_pct": improvement,
        }
        run_id = await conn.fetchval(
            INSERT_RUN, MODEL_NAME, version, len(train), cutoff, base_mae, model_mae,
            json.dumps(params), str(artifact.relative_to(REPO_ROOT)),
        )
        print(f"\nmodel_registry: id={run_id} name={MODEL_NAME} version={version} "
              f"artifact={artifact.relative_to(REPO_ROOT)}")
    finally:
        await conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--holdout-frac", type=float, default=0.2)
    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args().holdout_frac))
