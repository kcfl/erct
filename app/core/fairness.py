"""Cross-centre remedy fairness assessment module (Phase 3b-ii).

Pure functions and database layer evaluating compensation ratios and manual review coverage
disparities across centres for an exam.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from app.config import AppConfig, get_config


@dataclass
class CentreFairnessStats:
    centre_id: str
    rows: int
    share_manual_review: float
    share_extra_time: float
    mean_lost_s: float
    mean_extra_seconds: float
    compensation_ratio: Optional[float]


@dataclass
class FairnessFlag:
    kind: str  # 'ratio_spread' | 'coverage_gap'
    centres: List[str]
    values: Dict[str, Any]
    message: str


@dataclass
class FairnessEvaluation:
    status: str  # 'ok' | 'flagged'
    centres: Dict[str, CentreFairnessStats]
    flags: List[FairnessFlag] = field(default_factory=list)


def evaluate_fairness_pure(
    centre_rows: Dict[str, List[Dict[str, Any]]],
    cfg: AppConfig,
) -> FairnessEvaluation:
    """Pure function: evaluate fairness statistics and anomaly flags across centres."""
    min_rows = cfg.fairness.min_rows_per_centre
    max_ratio_spread = cfg.fairness.max_ratio_spread
    max_manual_gap = cfg.fairness.max_manual_review_gap

    stats_map: Dict[str, CentreFairnessStats] = {}

    for cid, rows in centre_rows.items():
        total_rows = len(rows)
        if total_rows == 0:
            continue

        mr_count = sum(1 for r in rows if r.get("remedy_recommended") == "manual_review")
        et_count = sum(1 for r in rows if r.get("remedy_recommended") == "extra_time")
        share_mr = round(mr_count / total_rows, 4)
        share_et = round(et_count / total_rows, 4)

        strong_rows = [r for r in rows if r.get("evidence_quality") == "strong"]
        if strong_rows:
            mean_lost = round(sum(r.get("lost_seconds", 0.0) for r in strong_rows) / len(strong_rows), 2)
            mean_extra = round(sum(r.get("extra_seconds", 0) for r in strong_rows) / len(strong_rows), 2)
        else:
            mean_lost = 0.0
            mean_extra = 0.0

        lost_gt_zero = [r for r in strong_rows if r.get("lost_seconds", 0.0) > 0.0]
        if lost_gt_zero:
            sum_extra = sum(r.get("extra_seconds", 0) for r in lost_gt_zero)
            sum_lost = sum(r.get("lost_seconds", 0.0) for r in lost_gt_zero)
            ratio = round(sum_extra / sum_lost, 4) if sum_lost > 0 else None
        else:
            ratio = None

        stats_map[cid] = CentreFairnessStats(
            centre_id=cid,
            rows=total_rows,
            share_manual_review=share_mr,
            share_extra_time=share_et,
            mean_lost_s=mean_lost,
            mean_extra_seconds=mean_extra,
            compensation_ratio=ratio,
        )

    return evaluate_fairness_stats(stats_map, cfg)


def evaluate_fairness_stats(
    stats_map: Dict[str, Any],
    cfg: AppConfig,
) -> FairnessEvaluation:
    """Pure function: evaluate fairness flags from per-centre statistics."""
    min_rows = cfg.fairness.min_rows_per_centre
    max_ratio_spread = cfg.fairness.max_ratio_spread
    max_manual_gap = cfg.fairness.max_manual_review_gap

    # Convert raw dicts to CentreFairnessStats if needed
    normalized: Dict[str, CentreFairnessStats] = {}
    for cid, s in stats_map.items():
        if isinstance(s, CentreFairnessStats):
            normalized[cid] = s
        elif isinstance(s, dict):
            normalized[cid] = CentreFairnessStats(
                centre_id=cid,
                rows=s.get("rows", 0),
                share_manual_review=s.get("share_manual_review", 0.0),
                share_extra_time=s.get("share_extra_time", 0.0),
                mean_lost_s=s.get("mean_lost_s", 0.0),
                mean_extra_seconds=s.get("mean_extra_seconds", 0.0),
                compensation_ratio=s.get("compensation_ratio"),
            )

    flags: List[FairnessFlag] = []

    # 1. Ratio spread check
    eligible_ratio = [
        s for s in normalized.values()
        if s.rows >= min_rows and s.compensation_ratio is not None and s.compensation_ratio > 0
    ]
    if len(eligible_ratio) >= 2:
        max_c = max(eligible_ratio, key=lambda s: s.compensation_ratio)
        min_c = min(eligible_ratio, key=lambda s: s.compensation_ratio)
        spread = round(max_c.compensation_ratio / min_c.compensation_ratio, 4)
        if spread > max_ratio_spread:
            flags.append(
                FairnessFlag(
                    kind="ratio_spread",
                    centres=[max_c.centre_id, min_c.centre_id],
                    values={
                        "max_ratio": max_c.compensation_ratio,
                        "min_ratio": min_c.compensation_ratio,
                        "spread": spread,
                        "threshold": max_ratio_spread,
                    },
                    message=(
                        f"Compensation ratio spread {spread:.2f} exceeds threshold "
                        f"{max_ratio_spread} between {max_c.centre_id} ({max_c.compensation_ratio:.2f}) "
                        f"and {min_c.centre_id} ({min_c.compensation_ratio:.2f})."
                    ),
                )
            )

    # 2. Coverage gap check (manual review share difference)
    eligible_coverage = [s for s in normalized.values() if s.rows >= min_rows]
    if len(eligible_coverage) >= 2:
        max_mr = max(eligible_coverage, key=lambda s: s.share_manual_review)
        min_mr = min(eligible_coverage, key=lambda s: s.share_manual_review)
        gap = round(max_mr.share_manual_review - min_mr.share_manual_review, 4)
        if gap > max_manual_gap:
            flags.append(
                FairnessFlag(
                    kind="coverage_gap",
                    centres=[max_mr.centre_id, min_mr.centre_id],
                    values={
                        "max_share": max_mr.share_manual_review,
                        "min_share": min_mr.share_manual_review,
                        "gap": gap,
                        "threshold": max_manual_gap,
                    },
                    message=(
                        f"Manual review coverage gap {gap:.2f} exceeds threshold "
                        f"{max_manual_gap} between {max_mr.centre_id} ({max_mr.share_manual_review:.1%}) "
                        f"and {min_mr.centre_id} ({min_mr.share_manual_review:.1%})."
                    ),
                )
            )

    status = "flagged" if flags else "ok"
    return FairnessEvaluation(status=status, centres=normalized, flags=flags)


# Convenient alias
evaluate_fairness = evaluate_fairness_pure


def compute_exam_fairness(conn: sqlite3.Connection, exam_id: str, cfg: Optional[AppConfig] = None) -> Dict[str, Any]:
    """Compute fairness report across all resolved incidents with impact computed for the given exam."""
    if cfg is None:
        cfg = get_config()

    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT ii.incident_id, ii.candidate_id, ii.lost_seconds, ii.extra_seconds,
               ii.evidence_quality, ii.remedy_recommended, inc.centre_id
        FROM incident_impacts ii
        JOIN incidents inc ON ii.incident_id = inc.incident_id
        WHERE inc.exam_id = ? AND inc.status = 'resolved' AND inc.impact_computed_at IS NOT NULL;
        """,
        (exam_id,),
    )
    rows = cursor.fetchall()
    cursor.close()

    centre_rows: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        cid = r["centre_id"]
        centre_rows.setdefault(cid, []).append(dict(r))

    evaluation = evaluate_fairness_pure(centre_rows, cfg)

    return {
        "exam_id": exam_id,
        "status": evaluation.status,
        "stats_per_centre": {
            cid: {
                "centre_id": s.centre_id,
                "rows": s.rows,
                "share_manual_review": s.share_manual_review,
                "share_extra_time": s.share_extra_time,
                "mean_lost_s": s.mean_lost_s,
                "mean_extra_seconds": s.mean_extra_seconds,
                "compensation_ratio": s.compensation_ratio,
            }
            for cid, s in evaluation.centres.items()
        },
        "flags": [
            {
                "kind": f.kind,
                "centres": f.centres,
                "values": f.values,
                "message": f.message,
            }
            for f in evaluation.flags
        ],
    }
