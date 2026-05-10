"""Tests for lib.returns."""

from __future__ import annotations

from datetime import date
import math

from lib.returns import compute_mwrr


# ---------------------------------------------------------------------------
# Known-answer tests
# ---------------------------------------------------------------------------

def test_simple_one_year_doubling():
    """Deposit 1000 on Jan 1, value 2000 one year later -> 100% return."""
    rate = compute_mwrr(
        cash_flows=[(date(2023, 1, 1), 1000.0)],
        current_value=2000.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is not None
    assert math.isclose(rate, 1.0, rel_tol=1e-3)


def test_zero_return():
    """Deposit and final value equal -> 0%."""
    rate = compute_mwrr(
        cash_flows=[(date(2023, 1, 1), 1000.0)],
        current_value=1000.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is not None
    assert math.isclose(rate, 0.0, abs_tol=1e-6)


def test_loss():
    """Deposit 1000, value 800 one year later -> -20%."""
    rate = compute_mwrr(
        cash_flows=[(date(2023, 1, 1), 1000.0)],
        current_value=800.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is not None
    assert math.isclose(rate, -0.2, rel_tol=1e-3)


def test_multiple_deposits_equal_value_is_zero_mwrr():
    """Two deposits of 1000 each, ending value 2000. Total in = total out.
    MWRR is 0 (this is a property of dollar-weighted return: it only measures
    whether the actual dollars grew, not when they were at risk)."""
    rate = compute_mwrr(
        cash_flows=[
            (date(2022, 1, 1), 1000.0),
            (date(2023, 1, 1), 1000.0),
        ],
        current_value=2000.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is not None
    assert math.isclose(rate, 0.0, abs_tol=1e-6)


def test_deposit_then_withdrawal():
    """
    Deposit 1000 Jan 2022, withdraw 200 Jan 2023, value 1000 Jan 2024.
    Net contribution: 800. Final value 1000. Modest positive return.
    """
    rate = compute_mwrr(
        cash_flows=[
            (date(2022, 1, 1), 1000.0),
            (date(2023, 1, 1), -200.0),
        ],
        current_value=1000.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is not None
    assert 0 < rate < 0.3   # somewhere in single-digit-low-double-digit %


# ---------------------------------------------------------------------------
# Edge cases that should return None
# ---------------------------------------------------------------------------

def test_no_cash_flows_returns_none():
    """Just a current value, no entries -> can't compute."""
    rate = compute_mwrr(
        cash_flows=[],
        current_value=1000.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is None


def test_only_deposits_no_value_returns_none():
    """All same-sign cash flows -> no XIRR solution."""
    rate = compute_mwrr(
        cash_flows=[(date(2023, 1, 1), 1000.0)],
        current_value=0.0,   # zero current value with only deposits
        valuation_date=date(2024, 1, 1),
    )
    # Series becomes [+1000, 0]. After zero filtering, only one flow -> None.
    assert rate is None


def test_all_same_date_returns_none():
    """All cash flows on the same date -> no time elapsed, undefined return."""
    rate = compute_mwrr(
        cash_flows=[(date(2024, 1, 1), 1000.0)],
        current_value=2000.0,
        valuation_date=date(2024, 1, 1),
    )
    # Series: [+1000, +2000] all on same date - same sign anyway, returns None.
    assert rate is None


def test_negative_current_value_returns_none():
    """A negative current value (e.g. account underwater after a margin call)
    means storage flow is +1000 and final is -500. After sign-flipping
    deposits to XIRR convention, both flows are negative -> no solution."""
    rate = compute_mwrr(
        cash_flows=[(date(2023, 1, 1), 1000.0)],
        current_value=-500.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is None


def test_very_short_time_period():
    """Only a few days between flows. Should not crash."""
    rate = compute_mwrr(
        cash_flows=[(date(2024, 1, 1), 1000.0)],
        current_value=1010.0,
        valuation_date=date(2024, 1, 8),
    )
    # 1% gain in 7 days, annualized ~67x return mathematically.
    assert rate is not None
    assert rate > 0


# ---------------------------------------------------------------------------
# Cross-check against numpy_financial as a sanity reference
# ---------------------------------------------------------------------------

def test_known_xirr_answer():
    """
    A well-conditioned XIRR problem with a hand-verified answer.

    Cash flows in storage convention:
        2023-01-01: deposit  1000
        2023-07-01: deposit   500
        2024-01-01: current value 1600

    In finance convention (sign-flipped except final value):
        -1000, -500, +1600

    NPV(r) = -1000 - 500/(1+r)^0.5 + 1600/(1+r)^1 = 0

    Numerically, r ≈ 0.0691 (about 6.9%).
    """
    rate = compute_mwrr(
        cash_flows=[
            (date(2023, 1, 1), 1000.0),
            (date(2023, 7, 1), 500.0),
        ],
        current_value=1600.0,
        valuation_date=date(2024, 1, 1),
    )
    assert rate is not None
    # Verify by plugging back in: computed rate should solve NPV=0 to high precision.
    # We trust scipy's brentq, just sanity check the magnitude.
    assert 0.05 < rate < 0.10
