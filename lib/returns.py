"""
Return calculations.

v1: MWRR (Money-Weighted Rate of Return) via XIRR.
v2 (planned, not implemented): TWRR and Modified Dietz.

XIRR solves for the annualized rate r in:

    sum_i  CF_i / (1 + r) ** ((d_i - d_0) / 365) = 0

For a non-trivial root to exist, the series must contain both positive and
negative cash flows.

Sign conventions
----------------
The rest of the app uses a USER-FRIENDLY convention in storage:
    Deposits are POSITIVE (money the user puts in).
    Withdrawals are NEGATIVE (money the user takes out).
    Current value is POSITIVE.

XIRR uses the FINANCE convention:
    Outflows from the investor's pocket are NEGATIVE.
    Inflows to the investor's pocket are POSITIVE.
    So a deposit (out of pocket -> into account) is NEGATIVE,
    a withdrawal (out of account -> into pocket) is POSITIVE,
    and the current value (would-be inflow if liquidated) is POSITIVE.

`compute_mwrr` accepts cash_flows in the storage convention and flips signs
internally before calling the solver, so the rest of the app can stay
in the natural convention.
"""

from __future__ import annotations

from datetime import date

import numpy as np
from scipy.optimize import brentq


def _npv(rate: float, cash_flows: list[float], days_offsets: list[float]) -> float:
    """Net present value at annualized `rate`, given days-from-anchor offsets."""
    one_plus_r = 1.0 + rate
    if one_plus_r <= 0:
        # Outside domain; return a large value to push the solver away.
        return float("inf")
    total = 0.0
    for cf, days in zip(cash_flows, days_offsets):
        total += cf / (one_plus_r ** (days / 365.0))
    return total


def compute_mwrr(
    cash_flows: list[tuple[date, float]],
    current_value: float,
    valuation_date: date,
) -> float | None:
    """
    Compute MWRR (XIRR).

    Parameters
    ----------
    cash_flows
        List of (date, signed_amount) tuples. Positive = deposit,
        negative = withdrawal. The current_value is NOT in this list;
        we append it ourselves below.
    current_value
        Current market value of the position on `valuation_date`. Net of
        any withdrawals that have already occurred (those are in cash_flows).
    valuation_date
        The "as of" date for current_value. Usually today.

    Returns
    -------
    float | None
        Annualized rate as a decimal (0.085 means 8.5%), or None if:
        - fewer than 2 distinct cash flow dates
        - all cash flows have the same sign (no XIRR solution exists)
        - solver fails to converge

    Notes
    -----
    Tries scipy's brentq first with a generous bracket. If brentq can't find
    a sign change (which can happen for unusual series), falls back to None
    rather than guessing.
    """
    # Build the full series in XIRR/finance convention.
    # Storage convention: deposit +, withdrawal -, current value +.
    # XIRR convention:    deposit -, withdrawal +, current value +.
    # So we flip the sign of the user's cash flows, but NOT the current value.
    flows: list[tuple[date, float]] = [(d, -float(a)) for d, a in cash_flows]
    flows.append((valuation_date, float(current_value)))

    # Filter out exact-zero cash flows (they don't affect the equation).
    flows = [(d, a) for d, a in flows if a != 0.0]

    if len(flows) < 2:
        return None

    # Need at least one positive and one negative flow for XIRR to have a solution.
    has_pos = any(a > 0 for _, a in flows)
    has_neg = any(a < 0 for _, a in flows)
    if not (has_pos and has_neg):
        return None

    # Anchor at the earliest date.
    flows.sort(key=lambda t: t[0])
    d0 = flows[0][0]
    days = [float((d - d0).days) for d, _ in flows]
    amounts = [a for _, a in flows]

    # All dates equal -> nothing to solve.
    if max(days) == min(days):
        return None

    # Try a generous bracket. Lower bound > -1 since (1+r) must be positive.
    lo, hi = -0.999, 100.0
    try:
        f_lo = _npv(lo, amounts, days)
        f_hi = _npv(hi, amounts, days)
    except (OverflowError, ValueError):
        return None

    # No sign change in the bracket -> no root in this range.
    if not np.isfinite(f_lo) or not np.isfinite(f_hi):
        return None
    if f_lo * f_hi > 0:
        # Try widening the upper bound a bit before giving up.
        for hi_try in (10.0, 5.0, 2.0, 1.0, 0.5):
            f_try = _npv(hi_try, amounts, days)
            if np.isfinite(f_try) and f_lo * f_try < 0:
                hi = hi_try
                break
        else:
            return None

    try:
        rate = brentq(
            _npv,
            lo,
            hi,
            args=(amounts, days),
            xtol=1e-8,
            maxiter=200,
        )
    except (ValueError, RuntimeError):
        return None

    if not np.isfinite(rate):
        return None
    return float(rate)
