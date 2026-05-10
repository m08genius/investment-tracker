"""Tests for lib.recurring."""

from __future__ import annotations

from datetime import date

import pytest

from lib.recurring import generate_dates


# Day-of-week reminders: 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri, 5=Sat, 6=Sun


# ---------------------------------------------------------------------------
# weekly
# ---------------------------------------------------------------------------

def test_weekly_starts_on_or_after_start_date():
    """Start Tue Jan 2; want Fridays. First occurrence is Fri Jan 5."""
    dates = generate_dates(
        "weekly",
        start=date(2024, 1, 2),   # Tuesday
        end=date(2024, 1, 31),
        day_of_week=4,            # Friday
    )
    assert dates == [
        date(2024, 1, 5),
        date(2024, 1, 12),
        date(2024, 1, 19),
        date(2024, 1, 26),
    ]


def test_weekly_when_start_is_target_dow():
    """Start on a Friday and want Fridays - first date is start itself."""
    dates = generate_dates(
        "weekly",
        start=date(2024, 1, 5),   # Friday
        end=date(2024, 1, 19),
        day_of_week=4,
    )
    assert dates == [date(2024, 1, 5), date(2024, 1, 12), date(2024, 1, 19)]


def test_weekly_returns_empty_if_no_dates_in_range():
    dates = generate_dates(
        "weekly",
        start=date(2024, 1, 2),
        end=date(2024, 1, 4),
        day_of_week=4,            # Friday is Jan 5, just past end
    )
    assert dates == []


def test_weekly_validates_dow():
    with pytest.raises(ValueError):
        generate_dates("weekly", date(2024, 1, 1), date(2024, 1, 31), day_of_week=7)
    with pytest.raises(ValueError):
        generate_dates("weekly", date(2024, 1, 1), date(2024, 1, 31), day_of_week=-1)


def test_weekly_requires_dow():
    with pytest.raises(ValueError, match="day_of_week"):
        generate_dates("weekly", date(2024, 1, 1), date(2024, 1, 31))


# ---------------------------------------------------------------------------
# biweekly
# ---------------------------------------------------------------------------

def test_biweekly_strides_two_weeks():
    dates = generate_dates(
        "biweekly",
        start=date(2024, 1, 1),
        end=date(2024, 2, 29),
        day_of_week=4,            # Friday
    )
    assert dates == [
        date(2024, 1, 5),
        date(2024, 1, 19),
        date(2024, 2, 2),
        date(2024, 2, 16),
    ]


# ---------------------------------------------------------------------------
# semi-monthly
# ---------------------------------------------------------------------------

def test_semi_monthly_1st_and_15th():
    dates = generate_dates(
        "semi_monthly",
        start=date(2024, 1, 1),
        end=date(2024, 3, 31),
    )
    assert dates == [
        date(2024, 1, 1),  date(2024, 1, 15),
        date(2024, 2, 1),  date(2024, 2, 15),
        date(2024, 3, 1),  date(2024, 3, 15),
    ]


def test_semi_monthly_respects_start_and_end():
    """Start mid-Jan, end mid-Feb -> only the 15th of Jan and 1st of Feb."""
    dates = generate_dates(
        "semi_monthly",
        start=date(2024, 1, 5),
        end=date(2024, 2, 14),
    )
    assert dates == [date(2024, 1, 15), date(2024, 2, 1)]


# ---------------------------------------------------------------------------
# monthly
# ---------------------------------------------------------------------------

def test_monthly_basic():
    dates = generate_dates(
        "monthly",
        start=date(2024, 1, 1),
        end=date(2024, 4, 30),
        day_of_month=15,
    )
    assert dates == [
        date(2024, 1, 15),
        date(2024, 2, 15),
        date(2024, 3, 15),
        date(2024, 4, 15),
    ]


def test_monthly_eom_clamp_for_31st_in_february():
    """Day-of-month 31 in February clamps to 28/29."""
    dates = generate_dates(
        "monthly",
        start=date(2024, 1, 1),
        end=date(2024, 4, 30),
        day_of_month=31,
    )
    assert dates == [
        date(2024, 1, 31),
        date(2024, 2, 29),    # 2024 leap year
        date(2024, 3, 31),
        date(2024, 4, 30),    # April has 30 days
    ]


def test_monthly_eom_clamp_non_leap_february():
    dates = generate_dates(
        "monthly",
        start=date(2023, 2, 1),
        end=date(2023, 3, 31),
        day_of_month=30,
    )
    assert dates == [date(2023, 2, 28), date(2023, 3, 30)]


def test_monthly_skips_month_when_day_falls_before_start():
    """day_of_month=10, but start=Jan 15 -> Jan's 10th is excluded."""
    dates = generate_dates(
        "monthly",
        start=date(2024, 1, 15),
        end=date(2024, 3, 31),
        day_of_month=10,
    )
    assert dates == [date(2024, 2, 10), date(2024, 3, 10)]


def test_monthly_validates_dom():
    with pytest.raises(ValueError):
        generate_dates(
            "monthly", date(2024, 1, 1), date(2024, 12, 31), day_of_month=0
        )
    with pytest.raises(ValueError):
        generate_dates(
            "monthly", date(2024, 1, 1), date(2024, 12, 31), day_of_month=32
        )


def test_monthly_requires_dom():
    with pytest.raises(ValueError, match="day_of_month"):
        generate_dates("monthly", date(2024, 1, 1), date(2024, 12, 31))


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------

def test_start_after_end_returns_empty():
    dates = generate_dates(
        "weekly",
        start=date(2024, 6, 1),
        end=date(2024, 1, 1),
        day_of_week=0,
    )
    assert dates == []


def test_unknown_frequency_raises():
    with pytest.raises(ValueError):
        generate_dates("yearly", date(2024, 1, 1), date(2024, 12, 31))
