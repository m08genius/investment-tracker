"""
Generate dates for recurring deposits.

Pure date arithmetic, no I/O. Kept separate from UI so the date logic
(EOM handling, biweekly stride, semi-monthly 1-and-15) is independently testable.

End date is always capped at today before generation.
"""

from __future__ import annotations

import calendar
from datetime import date, timedelta
from typing import Literal

Frequency = Literal["weekly", "biweekly", "semi_monthly", "monthly"]


def _last_day_of_month(year: int, month: int) -> int:
    return calendar.monthrange(year, month)[1]


def _safe_date(year: int, month: int, day: int) -> date:
    """Like date(year, month, day) but clamps day to month length (EOM rule)."""
    last = _last_day_of_month(year, month)
    return date(year, month, min(day, last))


def _add_months(d: date, months: int, *, day: int) -> date:
    """Add `months` to d and place it on `day`, clamped to month length."""
    total_month = d.month - 1 + months
    new_year = d.year + total_month // 12
    new_month = total_month % 12 + 1
    return _safe_date(new_year, new_month, day)


def generate_dates(
    frequency: Frequency,
    start: date,
    end: date,
    *,
    day_of_week: int | None = None,    # 0=Monday..6=Sunday, for weekly/biweekly
    day_of_month: int | None = None,   # 1..31, for monthly
) -> list[date]:
    """
    Generate occurrence dates for a recurring deposit.

    Parameters
    ----------
    frequency
        One of: "weekly", "biweekly", "semi_monthly", "monthly".
    start
        The first date the recurrence is allowed to occur on (inclusive).
    end
        The last date allowed (inclusive). The caller is expected to cap
        this at today before passing it in; this function does not look at
        date.today() itself.
    day_of_week
        Required for weekly/biweekly. 0=Monday..6=Sunday.
    day_of_month
        Required for monthly. 1..31. EOM convention applied for short months.

    Returns
    -------
    Sorted list of dates. Empty if nothing fits in [start, end].

    Conventions
    -----------
    - weekly:       every occurrence of `day_of_week` from start..end.
    - biweekly:     same as weekly but every other occurrence.
    - semi_monthly: 1st and 15th of each month, where each date is in [start, end].
    - monthly:      `day_of_month` of each month (clamped to month length), in [start, end].
    """
    if start > end:
        return []

    if frequency == "weekly":
        if day_of_week is None:
            raise ValueError("weekly frequency requires day_of_week (0=Mon..6=Sun).")
        return _generate_weekly(start, end, day_of_week, stride_weeks=1)

    if frequency == "biweekly":
        if day_of_week is None:
            raise ValueError("biweekly frequency requires day_of_week (0=Mon..6=Sun).")
        return _generate_weekly(start, end, day_of_week, stride_weeks=2)

    if frequency == "semi_monthly":
        return _generate_semi_monthly(start, end)

    if frequency == "monthly":
        if day_of_month is None:
            raise ValueError("monthly frequency requires day_of_month (1..31).")
        if not 1 <= day_of_month <= 31:
            raise ValueError("day_of_month must be between 1 and 31.")
        return _generate_monthly(start, end, day_of_month)

    raise ValueError(f"Unknown frequency: {frequency!r}")


def _generate_weekly(start: date, end: date, dow: int, *, stride_weeks: int) -> list[date]:
    if not 0 <= dow <= 6:
        raise ValueError("day_of_week must be 0 (Mon) through 6 (Sun).")
    # First occurrence on or after start that has weekday == dow.
    delta_days = (dow - start.weekday()) % 7
    first = start + timedelta(days=delta_days)
    if first > end:
        return []
    out: list[date] = []
    cur = first
    step = timedelta(weeks=stride_weeks)
    while cur <= end:
        out.append(cur)
        cur += step
    return out


def _generate_semi_monthly(start: date, end: date) -> list[date]:
    """1st and 15th of each month within the range."""
    out: list[date] = []
    # Walk from start's month to end's month inclusive.
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        for day in (1, 15):
            d = date(y, m, day)
            if start <= d <= end:
                out.append(d)
        # Increment month
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out


def _generate_monthly(start: date, end: date, dom: int) -> list[date]:
    """`dom` of each month (clamped to month length)."""
    out: list[date] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        d = _safe_date(y, m, dom)
        if start <= d <= end:
            out.append(d)
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return out
