# DECISIONS.md

1. **Indexes.** `attendance_logs`: unique `(emp_code, date)`, which enforces one record per day and serves monthly summaries, PATCH and the per-employee `$lookup`; `(date desc, emp_code)` for the attendance list sort and every month/range analytics match; `(status, date desc, emp_code)` for status filters; `(emp_code, punch_in desc)` for punch-out. `employees`: unique `emp_code`, `(department, emp_code)`, `(department, joined_on)` and `joined_on`. I rejected an index on `late_minutes`: the leaderboard already narrows by date first.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one`. MongoDB checks the unique index atomically, so one insert wins (201) and the other raises `DuplicateKeyError`, which I turn into 409. There is no read-then-write gap.

3. **Ties.** `$setWindowFields` with `$rank` gives 1, 2, 2, 4. I filter `rank <= limit` *after* ranking, so with `limit=3` and two people tied at rank 3, both are returned (four rows).

4. **Headcount.** The pipeline starts from `employees` (joined by month end), `$lookup`s their logs and unwinds with `preserveNullAndEmptyArrays`, so an employee with zero logs still appears once and is counted with `$addToSet`.

5. **100x data.** Pre-aggregate daily per-department counters on write (or a nightly `$merge`), so analytics read small summary collections instead of raw logs.
