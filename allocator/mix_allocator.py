# allocator/mix_allocator.py
from __future__ import annotations

import math
import hashlib
from collections import deque
from datetime import date
from typing import Dict, List
import pandas as pd


def _ceil_int(x: float) -> int:
    return int(math.ceil(x))


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


def _quotas_per_caller(caller_ids: List[str], per_caller_target: int, source_mix: Dict[str, float]) -> Dict[str, Dict[str, int]]:
    quotas = {c: {} for c in caller_ids}
    items = list(source_mix.items())
    for c in caller_ids:
        remaining = per_caller_target
        for i, (src, w) in enumerate(items):
            if w <= 0:
                q = 0
            elif i < len(items) - 1:
                q = _ceil_int(per_caller_target * float(w))
            else:
                q = max(0, remaining)
            quotas[c][src] = q
            remaining -= q
    return quotas


def allocate(
    candidates: pd.DataFrame,
    caller_ids: List[str],
    per_caller_target: int,
    source_mix: Dict[str, float],
) -> pd.DataFrame:
    if candidates is None or candidates.empty:
        return pd.DataFrame(columns=(list(candidates.columns) if candidates is not None else []) + ["caller_id"])
    if not caller_ids or per_caller_target <= 0 or not source_mix:
        out = candidates.copy()
        out["caller_id"] = None
        return out[out["caller_id"].notna()].reset_index(drop=True)

    # Per-source queues
    per_source = {src: deque(idxs.tolist()) for src, idxs in candidates.groupby("source_key").groups.items()}

    quotas = _quotas_per_caller(caller_ids, per_caller_target, source_mix)
    remaining_capacity = {c: sum(quotas[c].values()) for c in caller_ids}

    assigned_rows = []  # list of (row_idx, caller_id)
    today_str = date.today().isoformat()

    # 1) Primary per-source round-robin with per-source rotation
    for src in source_mix.keys():
        q = per_source.get(src, deque())
        if not q:
            continue
        offset = _stable_offset(f"{today_str}-{src}", len(caller_ids))
        rr_callers = deque(_rotate(caller_ids, offset))
        while q and any(quotas[c][src] > 0 for c in rr_callers):
            c = rr_callers[0]; rr_callers.rotate(-1)
            if quotas[c][src] <= 0:
                continue
            idx = q.popleft()
            assigned_rows.append((idx, c))
            quotas[c][src] -= 1
            remaining_capacity[c] -= 1

    # 2) Spillover: remaining candidates → remaining capacity, rotate caller order daily
    remaining_candidates = []
    for src in sorted(source_mix.keys(), key=lambda k: source_mix[k], reverse=True):
        qq = per_source.get(src, deque())
        if qq:
            remaining_candidates.extend(list(qq))
    rc_queue = deque(remaining_candidates)

    spill_offset = _stable_offset(f"{today_str}-spillover", len(caller_ids))
    rr_spill = deque(_rotate(caller_ids, spill_offset))

    while rc_queue and rr_spill:
        c = rr_spill[0]; rr_spill.rotate(-1)
        if remaining_capacity.get(c, 0) <= 0:
            continue
        idx = rc_queue.popleft()
        assigned_rows.append((idx, c))
        remaining_capacity[c] -= 1
        if all(v <= 0 for v in remaining_capacity.values()):
            break

    # Build preliminary output
    out = candidates.copy()
    out["caller_id"] = None
    for idx, c in assigned_rows:
        out.at[idx, "caller_id"] = c
    out = out[out["caller_id"].notna()].reset_index(drop=True)

    # 3) Final balancing pass: make totals differ by at most 1 (when feasible)
    if not out.empty and "caller_id" in out.columns:
        counts = out["caller_id"].value_counts().to_dict()
        total = len(out)
        n = len(caller_ids)
        base = total // n
        remainder = total % n

        # Build desired counts: pick which callers get the +1 via a stable daily order
        order = sorted(caller_ids, key=lambda x: _stable_offset(today_str + "-target-" + x, 1 << 16))
        plus_one_set = set(order[:remainder]) if remainder else set()
        desired = {c: base + (1 if c in plus_one_set else 0) for c in caller_ids}

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
                # move one row from donor to taker (pick last row from donor for minimal churn)
                rows = out.index[out["caller_id"] == d].tolist()
                if not rows:
                    continue
                ridx = rows[-1]
                out.at[ridx, "caller_id"] = t
                counts[d] -= 1
                counts[t] = counts.get(t, 0) + 1
                moved = True

    return out
