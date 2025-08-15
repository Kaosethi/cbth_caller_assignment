# allocator/mix_allocator.py
from __future__ import annotations

import hashlib
import math
from collections import deque
from datetime import date
from typing import Dict, List, Tuple

import pandas as pd


def _stable_offset(key: str, n: int) -> int:
    if n <= 0:
        return 0
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % n


def _rotate(xs: List[str], offset: int) -> List[str]:
    if not xs:
        return xs
    offset %= len(xs)
    return xs[offset:] + xs[:offset]


def _fair_round_split(total: int, weights: Dict[str, float]) -> Dict[str, int]:
    """
    Split 'total' across keys by weights with floor + largest-remainder method.
    Guarantees sum(result) == total and non-negative.
    """
    if total <= 0 or not weights:
        return {k: 0 for k in weights}

    # normalize weights to sum to 1 (clip negatives)
    wpos = {k: max(0.0, float(v)) for k, v in weights.items()}
    s = sum(wpos.values())
    if s <= 0:
        return {k: 0 for k in weights}
    wnorm = {k: v / s for k, v in wpos.items()}

    # floor and collect remainders
    base = {k: int(math.floor(total * wnorm[k])) for k in wnorm}
    assigned = sum(base.values())
    remaining = max(0, total - assigned)
    if remaining == 0:
        return base

    remainders = {k: (total * wnorm[k]) - base[k] for k in wnorm}
    # deterministic tie-breaker: sort by remainder desc, then by key
    order = sorted(wnorm.keys(), key=lambda k: (-remainders[k], k))
    for k in order[:remaining]:
        base[k] += 1
    return base


def _quotas_per_caller(
    caller_ids: List[str],
    per_caller_target: int,
    source_mix: Dict[str, float],
) -> Dict[str, Dict[str, int]]:
    """
    Return per-caller per-source quotas with exact total per caller == per_caller_target.
    """
    quotas: Dict[str, Dict[str, int]] = {}
    for c in caller_ids:
        quotas[c] = _fair_round_split(per_caller_target, source_mix)
    return quotas


def allocate(
    candidates: pd.DataFrame,
    caller_ids: List[str],
    per_caller_target: int,
    source_mix: Dict[str, float],
) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        cols = list(candidates.columns) if candidates is not None else []
        return pd.DataFrame(columns=cols + ["caller_id"])

    if not caller_ids or per_caller_target <= 0 or not source_mix:
        out = candidates.copy()
        out["caller_id"] = None
        # nothing to assign
        return out[out["caller_id"].notna()].reset_index(drop=True)

    # Group row indices by source_key
    per_source = {
        src: deque(idxs.tolist())
        for src, idxs in candidates.groupby("source_key").groups.items()
    }

    quotas = _quotas_per_caller(caller_ids, per_caller_target, source_mix)
    remaining_capacity = {c: sum(quotas[c].values()) for c in caller_ids}

    assigned_rows: List[Tuple[int, str]] = []
    today_str = date.today().isoformat()

    # 1) Primary per-source round-robin respecting per-source quotas
    for src in source_mix.keys():
        q = per_source.get(src, deque())
        if not q:
            continue

        # rotate callers per source/day for fairness
        offset = _stable_offset(f"{today_str}-{src}", len(caller_ids))
        rr_callers = deque(_rotate(caller_ids, offset))

        # keep assigning while there are rows and any caller has quota for this source
        while q and any(quotas[c].get(src, 0) > 0 for c in rr_callers):
            c = rr_callers[0]
            rr_callers.rotate(-1)

            if quotas[c].get(src, 0) <= 0 or remaining_capacity[c] <= 0:
                continue

            idx = q.popleft()
            assigned_rows.append((idx, c))
            quotas[c][src] -= 1
            remaining_capacity[c] -= 1

    # 2) Spillover: assign remaining candidates to any caller with remaining capacity
    remaining_candidates = []
    for src in sorted(source_mix.keys(), key=lambda k: source_mix[k], reverse=True):
        qq = per_source.get(src, deque())
        if qq:
            remaining_candidates.extend(list(qq))
    rc_queue = deque(remaining_candidates)

    spill_offset = _stable_offset(f"{today_str}-spillover", len(caller_ids))
    rr_spill = deque(_rotate(caller_ids, spill_offset))

    while rc_queue and rr_spill:
        c = rr_spill[0]
        rr_spill.rotate(-1)
        if remaining_capacity.get(c, 0) <= 0:
            continue
        idx = rc_queue.popleft()
        assigned_rows.append((idx, c))
        remaining_capacity[c] -= 1
        if all(v <= 0 for v in remaining_capacity.values()):
            break

    # Build output
    out = candidates.copy()
    out["caller_id"] = None
    for idx, c in assigned_rows:
        out.at[idx, "caller_id"] = c
    out = out[out["caller_id"].notna()].reset_index(drop=True)

    # 3) Final balancing: keep variance ≤ 1 when feasible
    if not out.empty and "caller_id" in out.columns:
        counts = out["caller_id"].value_counts().reindex(caller_ids).fillna(0).astype(int).to_dict()
        total = len(out)
        n = len(caller_ids)
        base = total // n
        remainder = total % n

        # choose who gets +1 today in a stable way
        order = sorted(caller_ids, key=lambda x: _stable_offset(today_str + "-target-" + x, 1 << 16))
        desired = {c: base + (1 if i < remainder else 0) for i, c in enumerate(order)}

        def overfull():
            return [c for c in caller_ids if counts.get(c, 0) > desired[c]]

        def underfull():
            return [c for c in caller_ids if counts.get(c, 0) < desired[c]]

        moved = True
        while moved:
            moved = False
            donors = overfull()
            takers = underfull()
            if not donors or not takers:
                break
            for d, t in zip(donors, takers):
                rows = out.index[out["caller_id"] == d].tolist()
                if not rows:
                    continue
                ridx = rows[-1]  # minimal churn
                out.at[ridx, "caller_id"] = t
                counts[d] -= 1
                counts[t] = counts.get(t, 0) + 1
                moved = True

    return out
