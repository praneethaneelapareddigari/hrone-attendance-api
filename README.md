# Employee Attendance & Analytics API

A FastAPI + MongoDB service for employee punch-in/punch-out, manual corrections with an audit trail, and
attendance analytics built on MongoDB aggregation pipelines.

**Status: complete.** Every endpoint in the contract is implemented; nothing is missing.

## Requirements

- Python 3.11+
- MongoDB 6.0+ (tested on MongoDB 7.0)

## Run

```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt

export MONGO_URI=mongodb://localhost:27017
export MONGO_DB=attendance_db

python sample_seed.py                # optional: loads the nine sample documents
uvicorn app.main:app --port 8000
```

- Configuration comes from the `MONGO_URI` and `MONGO_DB` environment variables. A local `.env` file also works
  (see `.env.example`), but real environment variables always take priority. The app refuses to start if either
  is missing; nothing is hard-coded.
- All indexes are created automatically at startup and creating them is idempotent. No manual steps.
- Health check: `GET /health` returns 200 when MongoDB answers, 503 otherwise.
- Interactive API docs: http://localhost:8000/docs

## What is implemented

| Area | Endpoints |
|---|---|
| System | `GET /health` |
| Employees | `POST /employees`, `GET /employees` |
| Attendance | `POST /attendance/punch-in`, `POST /attendance/punch-out`, `GET /attendance`, `PATCH /attendance/{emp_code}/{date}` |
| Analytics | `GET /analytics/employees/{emp_code}/monthly`, `GET /analytics/departments/summary`, `GET /analytics/leaderboard/late`, `GET /analytics/departments/{department}/trend` |
| Admin | `GET /admin/explain/{endpoint}` |

## Project layout

```
app/main.py            all application code, in nine numbered sections
REVIEW.md              Part A: defects found in the starter and how each was fixed
DECISIONS.md           Part C: indexes, concurrency, ties, headcount, scaling
tests/test_api.py      end-to-end tests against a real MongoDB
scripts/load_100k.py   generates ~100,000 attendance records for speed / explain checks
sample_seed.py         loads the nine sample documents
```

## Tests

```bash
export MONGO_URI=mongodb://localhost:27017
export MONGO_DB=attendance_test      # must end in _test: the tests drop this database
python -m pytest -q
```

All 17 tests pass on MongoDB 7.0. They use hand-computed expected values and cover:

- the business rules R1-R5: the 10-minute grace boundary, the 30-minute overtime minimum, half-up rounding,
  half days and overnight shifts;
- concurrency: parallel punch-ins, punch-outs, employee creates and corrections;
- analytics edge cases: leaderboard ties at the cutoff, employees with no logs, mid-month and future joiners,
  weekend records, logs for unknown employees, and trend gap filling with the 7-day moving average;
- explain plans: every analytics query uses an index (IXSCAN) and never a full collection scan (COLLSCAN).

On 100,000 records (`scripts/load_100k.py`), every endpoint responds in under 0.25 s.

## Design notes and interpretations

- **Concurrency.** Duplicate punch-ins and employee creates are rejected by unique indexes, never by
  check-then-insert. Punch-out and corrections are single atomic `find_one_and_update` calls that only match the
  state that was read, so the losing request of a race gets a 409 and no history entry is ever lost.
- **Rounding.** Reported numbers are rounded half-up inside the pipelines using Decimal128, because MongoDB's
  `$round` rounds half to even.
- **Working days.** `working_days` in the monthly summary is pure calendar arithmetic: Mon-Fri counted from
  `max(month start, joined_on)`. It is computed in Python and passed into the pipeline. Everything that reads
  attendance data is a MongoDB aggregation.
- **"Changes nothing" (PATCH).** A correction where `status`, `punch_in` and `punch_out` would all stay the same
  is rejected with 422.
- **Trend range.** "Exceeds 92 days" means more than 92 calendar days inclusive, so 1 Jul - 30 Sep (92 days) is
  allowed and 1 Jul - 1 Oct is rejected.