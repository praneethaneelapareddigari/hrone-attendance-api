"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (real environment variables win over a local .env)

Layout of this file (everything lives here, as the assignment requires):
  1. Configuration and database connection
  2. Index creation (runs at startup, idempotent)
  3. Time helpers and business rules R1-R5 (pure functions, no database access)
  4. Request models and validation
  5. Serialisation (MongoDB document -> API JSON)
  6. Employee and attendance endpoints
  7. Aggregation pipeline builders (shared by the analytics and the explain endpoints)
  8. Analytics endpoints
  9. Explain endpoint
"""
import calendar
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import json_util
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration and database connection
# --------------------------------------------------------------------------- #
# load_dotenv() never overrides variables that are already set, so real environment variables win.
load_dotenv(override=False)

MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB")
if not MONGO_URI or not MONGO_DB:
    raise RuntimeError("MONGO_URI and MONGO_DB must be set (environment variables or a local .env file)")

# tz_aware=True: PyMongo returns timezone-aware UTC datetimes instead of naive ones, so arithmetic
# with IST-aware datetimes is always correct and we never depend on the server's local time zone.
client = MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=5000)
db = client[MONGO_DB]
employees = db["employees"]
logs = db["attendance_logs"]

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")
NON_PRESENCE_STATUSES = ("ABSENT", "LEAVE")
GRACE_SECONDS = 10 * 60          # R2: late only if strictly more than 10:00 after shift start
MIN_OVERTIME_MINUTES = 30        # R3
HALF_DAY_BELOW_HOURS = Decimal("4.50")  # R5
MAX_SHIFT = timedelta(hours=24)  # punch_out must be within 24 h of punch_in
MAX_TREND_DAYS = 92

MIN_EPOCH_MS = 100_000_000_000      # anything smaller looks like seconds -> 422
MAX_EPOCH_MS = 4_102_444_800_000    # 2100-01-01

Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]


# --------------------------------------------------------------------------- #
# 2. Indexes
# --------------------------------------------------------------------------- #
def ensure_indexes() -> None:
    """Idempotent: create_index is a no-op when an identical index already exists."""
    # Uniqueness of the business key; makes concurrent duplicate creates fail with DuplicateKeyError.
    employees.create_index([("emp_code", ASCENDING)], unique=True, name="uniq_emp_code")
    # GET /employees?department=X sorted by emp_code; trend + summary filtered by department.
    employees.create_index([("department", ASCENDING), ("emp_code", ASCENDING)], name="department_emp_code")
    employees.create_index([("department", ASCENDING), ("joined_on", ASCENDING)], name="department_joined_on")
    # Department summary without a department filter starts with {joined_on: {$lte: end}}.
    employees.create_index([("joined_on", ASCENDING)], name="joined_on")

    # At most one record per (emp_code, date). This is what makes punch-in race-safe, and it also serves
    # PATCH lookups, the employee monthly summary and the per-employee $lookup in the department analytics.
    logs.create_index([("emp_code", ASCENDING), ("date", ASCENDING)], unique=True, name="uniq_emp_code_date")
    # GET /attendance default sort (date desc, emp_code asc) and every date-range analytics query.
    logs.create_index([("date", DESCENDING), ("emp_code", ASCENDING)], name="date_desc_emp_code")
    # GET /attendance?status=X keeps the same sort without scanning other statuses.
    logs.create_index([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], name="status_date_emp_code")
    # Punch-out: "the employee's most recent record whose punch_in <= punched_at".
    logs.create_index([("emp_code", ASCENDING), ("punch_in", DESCENDING)], name="emp_code_punch_in_desc")


_indexes_ready = False


def _try_ensure_indexes() -> bool:
    global _indexes_ready
    if not _indexes_ready:
        try:
            ensure_indexes()
            _indexes_ready = True
        except PyMongoError as exc:  # MongoDB not reachable yet: /health retries later
            log.warning("could not create indexes yet: %s", exc)
    return _indexes_ready


@asynccontextmanager
async def lifespan(_: FastAPI):
    _try_ensure_indexes()
    yield
    client.close()


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 3. Time helpers and business rules (pure functions)
# --------------------------------------------------------------------------- #
def reject(msg: str, loc: tuple = ("body",)) -> None:
    """Raise a 422 with FastAPI's default validation body ({"detail": [ ... ]})."""
    raise RequestValidationError([{"type": "value_error", "loc": list(loc), "msg": msg, "input": None}])


def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    if value is None:
        return None
    if value.tzinfo is None:  # defensive: BSON datetimes are always UTC
        value = value.replace(tzinfo=UTC)
    # Integer arithmetic on timedeltas: exact, no float rounding.
    return (value - EPOCH) // timedelta(milliseconds=1)


def truncate_seconds(value: Optional[datetime]) -> Optional[datetime]:
    return None if value is None else value.replace(microsecond=0)


def now_utc_seconds() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def parse_hhmm(value: str) -> time:
    h, m = value.split(":")
    return time(int(h), int(m))


def is_overnight(emp: dict) -> bool:
    # "HH:MM" strings compare correctly as text.
    return emp["shift_end"] <= emp["shift_start"]


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: the IST calendar date of punch_in; for an overnight shift, a punch-in before shift_end
    belongs to the shift that started the previous day."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp) and local.time() < parse_hhmm(emp["shift_end"]):
        day -= timedelta(days=1)
    return day


def shift_start_at(day: date, emp: dict) -> datetime:
    return datetime.combine(day, parse_hhmm(emp["shift_start"]), tzinfo=IST)


def shift_end_at(day: date, emp: dict) -> datetime:
    end_day = day + timedelta(days=1) if is_overnight(emp) else day
    return datetime.combine(end_day, parse_hhmm(emp["shift_end"]), tzinfo=IST)


def late_minutes(punch_in: datetime, day: date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then floor minutes since shift start."""
    seconds = int((truncate_seconds(punch_in) - shift_start_at(day, emp)).total_seconds())
    return seconds // 60 if seconds > GRACE_SECONDS else 0


def overtime_minutes(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: floor minutes after shift end, only when at least 30."""
    seconds = int((truncate_seconds(punch_out) - shift_end_at(day, emp)).total_seconds())
    minutes = seconds // 60 if seconds > 0 else 0
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def work_hours(punch_in: datetime, punch_out: datetime) -> Decimal:
    """R4: seconds / 3600, rounded half-up to 2 decimals using exact decimal arithmetic."""
    seconds = int((truncate_seconds(punch_out) - truncate_seconds(punch_in)).total_seconds())
    return (Decimal(seconds) / Decimal(3600)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def derived_fields(status: str, punch_in: Optional[datetime], punch_out: Optional[datetime],
                   day: date, emp: dict) -> dict:
    """Every derived value for a record, per the table in DATA_MODEL.md section 3."""
    if status not in PRESENCE_STATUSES or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    result = {"work_hours": None, "late_minutes": late_minutes(punch_in, day, emp),
              "overtime_minutes": 0, "half_day": False}
    if punch_out is not None:
        hours = work_hours(punch_in, punch_out)
        result.update(work_hours=float(hours), overtime_minutes=overtime_minutes(punch_out, day, emp),
                      half_day=hours < HALF_DAY_BELOW_HOURS)  # R5 uses the *rounded* hours
    return result


def parse_date(value: str, loc: tuple) -> date:
    try:
        if len(value) != 10:
            raise ValueError
        return date.fromisoformat(value)
    except ValueError:
        reject("must be a calendar date YYYY-MM-DD", loc)


def month_bounds(month: str) -> tuple[str, str]:
    year, mon = int(month[:4]), int(month[5:7])
    last = calendar.monthrange(year, mon)[1]
    return f"{month}-01", f"{month}-{last:02d}"


def count_weekdays(first: date, last: date) -> int:
    """Mon-Fri days in [first, last], inclusive (R7). 0 if the range is empty."""
    if last < first:
        return 0
    total_days = (last - first).days + 1
    full_weeks, rest = divmod(total_days, 7)
    count = full_weeks * 5
    for i in range(rest):
        if (first + timedelta(days=full_weeks * 7 + i)).weekday() < 5:
            count += 1
    return count


# --------------------------------------------------------------------------- #
# 4. Request models
# --------------------------------------------------------------------------- #
# strict=True: floats (1.5e12), strings ("1783312500000") and booleans are rejected, not coerced.
EpochMs = Annotated[int, Field(strict=True, ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]
HHMM = Annotated[str, Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")]
MonthParam = Annotated[str, Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")]
PageParam = Annotated[int, Query(ge=1)]
PageSizeParam = Annotated[int, Query(ge=1, le=100)]


class EmployeeCreate(BaseModel):
    model_config = ConfigDict(extra="ignore")

    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: str

    @field_validator("joined_on")
    @classmethod
    def _valid_date(cls, value: str) -> str:
        if len(value) != 10:
            raise ValueError("must be a calendar date YYYY-MM-DD")
        date.fromisoformat(value)  # raises ValueError -> 422
        return value

    @model_validator(mode="after")
    def _shift_not_empty(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: str
    punched_at: Optional[EpochMs] = None


class RegularizeRequest(BaseModel):
    # Derived fields (work_hours, late_minutes, ...) are not declared, so they are ignored if sent.
    model_config = ConfigDict(extra="ignore")

    status: Optional[Status] = None
    punch_in: Optional[EpochMs] = None
    punch_out: Optional[EpochMs] = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def _no_explicit_nulls(self):
        # None means "not supplied"; an explicit JSON null is not a valid value for these fields.
        for name in ("status", "punch_in", "punch_out"):
            if name in self.model_fields_set and getattr(self, name) is None:
                raise ValueError(f"{name} may not be null")
        return self


# --------------------------------------------------------------------------- #
# 5. Serialisation
# --------------------------------------------------------------------------- #
EMPLOYEE_FIELDS = ("emp_code", "name", "email", "department", "shift_start", "shift_end", "joined_on")
TIME_FIELDS = ("punch_in", "punch_out")


def employee_out(doc: dict) -> dict:
    out = {k: doc.get(k) for k in EMPLOYEE_FIELDS}
    out["created_at"] = dt_to_ms(doc.get("created_at"))
    return out


def history_out(entry: dict) -> dict:
    changes = {}
    for field, change in (entry.get("changes") or {}).items():
        if field in TIME_FIELDS:
            change = {"from": dt_to_ms(change.get("from")), "to": dt_to_ms(change.get("to"))}
        changes[field] = change
    return {"at": dt_to_ms(entry.get("at")), "by": entry.get("by"), "reason": entry.get("reason"), "changes": changes}


def record_out(doc: dict) -> dict:
    """Missing optional fields (legacy seeded data) are reported with their defaults."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day") or False),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


def get_employee_or_404(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code}, {"_id": 0})
    if emp is None:
        raise HTTPException(404, f"employee {emp_code} not found")
    return emp


# --------------------------------------------------------------------------- #
# 6. Employee and attendance endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, "MongoDB is not reachable")
    _try_ensure_indexes()
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    now = datetime.now(UTC)
    doc = body.model_dump()
    doc["created_at"] = now.replace(microsecond=now.microsecond // 1000 * 1000)  # BSON keeps ms precision
    try:
        # No check-then-insert: the unique index decides, so concurrent duplicates get exactly one 201.
        employees.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, f"emp_code {body.emp_code} already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(department: Optional[str] = None, page: PageParam = 1, page_size: PageSizeParam = 20):
    query = {"department": department} if department is not None else {}
    cursor = (employees.find(query, {"_id": 0}).sort("emp_code", ASCENDING)
              .skip((page - 1) * page_size).limit(page_size))
    return {"items": [employee_out(d) for d in cursor], "total": employees.count_documents(query),
            "page": page, "page_size": page_size}


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc_seconds()
    day = attendance_date(ts, emp)
    doc = {"emp_code": emp["emp_code"], "date": day.isoformat(), "status": body.status,
           "punch_in": ts, "punch_out": None,
           **derived_fields(body.status, ts, None, day, emp), "history": []}
    try:
        # Race-safe: the unique (emp_code, date) index lets exactly one concurrent insert succeed.
        logs.insert_one(doc)
    except DuplicateKeyError:
        raise HTTPException(409, f"{emp['emp_code']} already has a record for {day.isoformat()}")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee_or_404(body.emp_code)
    ts = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc_seconds()
    # Most recent record whose punch_in <= punched_at; works across midnight because it is not keyed on date.
    record = logs.find_one({"emp_code": emp["emp_code"], "punch_in": {"$lte": ts}},
                           sort=[("punch_in", DESCENDING)])
    if record is None:
        raise HTTPException(404, "no punch-in found")
    if record.get("punch_out") is not None:
        raise HTTPException(409, "this record is already punched out")
    punched_in = truncate_seconds(record["punch_in"])
    if ts <= punched_in or ts - punched_in > MAX_SHIFT:
        reject("punched_at must be after punch_in and within 24 hours of it", ("body", "punched_at"))

    day = date.fromisoformat(record["date"])
    derived = derived_fields(record["status"], punched_in, ts, day, emp)
    # Atomic compare-and-set: only a record that is still open can be closed, so of two simultaneous
    # punch-outs exactly one matches; the other gets None back and returns 409.
    updated = logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": {"punch_out": ts, "work_hours": derived["work_hours"],
                  "overtime_minutes": derived["overtime_minutes"], "half_day": derived["half_day"]}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(409, "this record is already punched out")
    return record_out(updated)


def attendance_list_query(emp_code, date_from, date_to, status) -> dict:
    query: dict[str, Any] = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        start = parse_date(date_from, ("query", "date_from")) if date_from is not None else None
        end = parse_date(date_to, ("query", "date_to")) if date_to is not None else None
        if start and end and start > end:
            reject("date_from must not be after date_to", ("query", "date_from"))
        query["date"] = {}
        if start:
            query["date"]["$gte"] = start.isoformat()
        if end:
            query["date"]["$lte"] = end.isoformat()
    if status is not None:
        query["status"] = status
    return query


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance")
def list_attendance(emp_code: Optional[str] = None, date_from: Optional[str] = None,
                    date_to: Optional[str] = None, status: Optional[Status] = None,
                    page: PageParam = 1, page_size: PageSizeParam = 20):
    query = attendance_list_query(emp_code, date_from, date_to, status)
    # Sorting, skipping and limiting happen in MongoDB on an index; nothing is loaded into memory.
    cursor = logs.find(query, {"_id": 0}).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    return {"items": [record_out(d) for d in cursor], "total": logs.count_documents(query),
            "page": page, "page_size": page_size}


@app.patch("/attendance/{emp_code}/{date}")
def regularize(body: RegularizeRequest, emp_code: str, date_str: str = Path(alias="date")):
    day = parse_date(date_str, ("path", "date"))
    emp = get_employee_or_404(emp_code)
    current = logs.find_one({"emp_code": emp_code, "date": day.isoformat()})
    if current is None:
        raise HTTPException(404, f"no record for {emp_code} on {day.isoformat()}")

    old = {
        "status": current["status"],
        "punch_in": current.get("punch_in"),
        "punch_out": current.get("punch_out"),
        "work_hours": current.get("work_hours"),
        "late_minutes": current.get("late_minutes") or 0,
        "overtime_minutes": current.get("overtime_minutes") or 0,
        "half_day": bool(current.get("half_day") or False),
    }

    new_status = body.status or old["status"]
    if new_status in NON_PRESENCE_STATUSES:
        if body.punch_in is not None or body.punch_out is not None:
            reject(f"{new_status} clears both punch times; do not send punch_in or punch_out")
        new_in = new_out = None
    else:
        new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else truncate_seconds(old["punch_in"])
        new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else truncate_seconds(old["punch_out"])
        if new_in is None:
            reject(f"status {new_status} requires a punch_in", ("body", "punch_in"))
        if body.punch_in is not None and attendance_date(new_in, emp) != day:
            reject("punch_in must fall on the record's attendance date", ("body", "punch_in"))
        if new_out is not None and (new_out <= new_in or new_out - new_in > MAX_SHIFT):
            reject("punch_out must be after punch_in and within 24 hours of it", ("body", "punch_out"))

    new = {"status": new_status, "punch_in": new_in, "punch_out": new_out,
           **derived_fields(new_status, new_in, new_out, day, emp)}

    if all(new[k] == old[k] for k in ("status", "punch_in", "punch_out")):
        reject("the request does not change anything")

    changes = {k: {"from": old[k], "to": new[k]} for k in new if new[k] != old[k]}
    entry = {"at": now_utc_seconds(), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: the update only applies if the record is still exactly what we read
    # (same punch times/status and same number of history entries). A concurrent PATCH or punch-out
    # makes this match nothing, and the loser gets a 409 instead of silently overwriting.
    guard = {
        "_id": current["_id"],
        "status": current["status"],
        "punch_in": current.get("punch_in"),
        "punch_out": current.get("punch_out"),
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(current.get("history") or [])]},
    }
    updated = logs.find_one_and_update(guard, {"$set": new, "$push": {"history": entry}},
                                       return_document=ReturnDocument.AFTER)
    if updated is None:
        raise HTTPException(409, "the record was changed by another request; reload and retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 7. Aggregation pipeline builders
# --------------------------------------------------------------------------- #
# Each builder returns (collection, pipeline). The analytics endpoint runs it and the explain endpoint
# explains exactly the same thing, so the two can never drift apart.

def round_half_up(expr: Any, places: int) -> dict:
    """Half-up rounding inside MongoDB. $round rounds half to even, so it can't be used for R8.
    Works in Decimal128 to avoid binary float error (2.675 must become 2.68). Null stays null."""
    factor = 10 ** places
    return {"$divide": [
        {"$floor": {"$add": [{"$multiply": [{"$toDecimal": expr}, factor]}, Decimal128("0.5")]}},
        factor,
    ]}


def to_double(expr: Any) -> dict:
    return {"$toDouble": expr}


def record_facts_stage() -> dict:
    """Turns each attendance record into the numbers the analytics add up.
    Reads the *stored* derived values (missing late/overtime/half_day count as 0/0/false)."""
    present = {"$in": ["$status", list(PRESENCE_STATUSES)]}
    half_day = {"$eq": [{"$ifNull": ["$half_day", False]}, True]}
    presence_units = {"$cond": [present, {"$cond": [half_day, 0.5, 1]}, 0]}
    weekday = {"$in": [
        {"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d", "onError": None}}},
        [1, 2, 3, 4, 5],
    ]}
    late = {"$ifNull": ["$late_minutes", 0]}
    return {"$project": {
        "_id": 0,
        "emp_code": 1,
        "date": 1,
        "presence_units": presence_units,                           # any day (trend)
        "weekday_units": {"$cond": [weekday, presence_units, 0]},   # Mon-Fri only (present_days, R7)
        "late": late,
        "is_late": {"$cond": [{"$gt": [late, 0]}, 1, 0]},
        "ot": {"$ifNull": ["$overtime_minutes", 0]},
        # work_hours only for presence records that have one (department avg_work_hours)
        "wh": {"$cond": [{"$and": [present, {"$isNumber": "$work_hours"}]}, {"$toDecimal": "$work_hours"}, None]},
        "is_leave": {"$cond": [{"$eq": ["$status", "LEAVE"]}, 1, 0]},
        "is_on_duty": {"$cond": [{"$eq": ["$status", "ON_DUTY"]}, 1, 0]},
    }}


def employee_monthly_pipeline(emp_code: str, month: str, working_days: int) -> tuple[str, list]:
    start, end = month_bounds(month)
    pipeline = [
        {"$match": {"emp_code": emp_code, "date": {"$gte": start, "$lte": end}}},
        record_facts_stage(),
        {"$group": {
            "_id": None,
            "present_days": {"$sum": "$weekday_units"},
            "leave_days": {"$sum": "$is_leave"},
            "late_count": {"$sum": "$is_late"},
            "total_late_minutes": {"$sum": "$late"},
            "total_overtime_minutes": {"$sum": "$ot"},
        }},
        {"$project": {
            "_id": 0,
            "present_days": to_double(round_half_up("$present_days", 2)),
            "leave_days": 1, "late_count": 1, "total_late_minutes": 1, "total_overtime_minutes": 1,
            "attendance_pct": to_double(round_half_up(
                {"$divide": [{"$multiply": [{"$toDecimal": "$present_days"}, 100]}, working_days]}, 2))
            if working_days > 0 else {"$literal": None},
        }},
    ]
    return "attendance_logs", pipeline


def department_summary_pipeline(month: str, department: Optional[str]) -> tuple[str, list]:
    start, end = month_bounds(month)
    # Start from employees, not from logs: that is how people with zero logs still get counted (R9).
    match: dict[str, Any] = {"joined_on": {"$lte": end}}
    if department is not None:
        match["department"] = department
    pipeline = [
        {"$match": match},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",   # served by the (emp_code, date) index
            "pipeline": [{"$match": {"date": {"$gte": start, "$lte": end}}}, record_facts_stage()],
            "as": "log",
        }},
        # Keep employees with no logs: they appear once with "log" missing, adding to headcount only.
        {"$unwind": {"path": "$log", "preserveNullAndEmptyArrays": True}},
        {"$group": {
            "_id": "$department",
            "employees": {"$addToSet": "$emp_code"},
            "present_days": {"$sum": "$log.weekday_units"},
            "work_hours_sum": {"$sum": "$log.wh"},
            "work_hours_records": {"$sum": {"$cond": [{"$isNumber": "$log.wh"}, 1, 0]}},
            "late_count": {"$sum": "$log.is_late"},
            "total_late_minutes": {"$sum": "$log.late"},
            "leave_count": {"$sum": "$log.is_leave"},
            "on_duty_count": {"$sum": "$log.is_on_duty"},
        }},
        {"$project": {
            "_id": 0,
            "department": "$_id",
            "headcount": {"$size": "$employees"},
            "present_days": to_double(round_half_up("$present_days", 2)),
            # Mean over records (sum / count), not a mean of per-employee means.
            "avg_work_hours": {"$cond": [
                {"$gt": ["$work_hours_records", 0]},
                to_double(round_half_up({"$divide": ["$work_hours_sum", "$work_hours_records"]}, 2)),
                None,
            ]},
            "late_count": 1, "total_late_minutes": 1, "leave_count": 1, "on_duty_count": 1,
        }},
        {"$sort": {"department": 1}},
    ]
    return "employees", pipeline


def late_leaderboard_pipeline(month: str, limit: int, department: Optional[str]) -> tuple[str, list]:
    start, end = month_bounds(month)
    pipeline: list[dict] = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        # Inner join: logs whose employee does not exist disappear here.
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},
    ]
    if department is not None:
        pipeline.append({"$match": {"emp.department": department}})  # rank inside the department
    pipeline += [
        # $rank is standard competition ranking: ties share a rank and the next rank is skipped (1, 2, 2, 4).
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        {"$match": {"rank": {"$lte": limit}}},  # limit applies after ranking, so ties at the cutoff are kept
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {"_id": 0, "rank": 1, "emp_code": "$_id", "name": "$emp.name",
                      "department": "$emp.department", "total_late_minutes": 1, "late_count": 1}},
    ]
    return "attendance_logs", pipeline


def department_trend_pipeline(department: str, first: date, last: date) -> tuple[str, list]:
    start, end = first.isoformat(), last.isoformat()
    lower = datetime.combine(first, time(), tzinfo=UTC)
    upper = datetime.combine(last + timedelta(days=1), time(), tzinfo=UTC)  # $densify upper bound is exclusive
    pipeline = [
        {"$match": {"department": department}},
        {"$lookup": {
            "from": "attendance_logs",
            "localField": "emp_code",
            "foreignField": "emp_code",
            "pipeline": [{"$match": {"date": {"$gte": start, "$lte": end}}}, record_facts_stage()],
            "as": "logs",
        }},
        # Every employee becomes "events" keyed by day: +1 headcount on the day they count from
        # (joined_on, or the first day of the range if they joined earlier), plus one event per log.
        {"$project": {"_id": 0, "events": {"$concatArrays": [
            [{"$cond": [
                {"$lte": ["$joined_on", end]},
                {"date": {"$max": ["$joined_on", start]}, "hc": 1, "p": 0, "l": 0},
                {"date": start, "hc": 0, "p": 0, "l": 0},   # joins after the range: no headcount
            ]}],
            {"$map": {"input": "$logs", "as": "r",
                      "in": {"date": "$$r.date", "hc": 0, "p": "$$r.presence_units", "l": "$$r.is_late"}}},
        ]}}},
        {"$unwind": "$events"},
        {"$group": {"_id": "$events.date", "joined": {"$sum": "$events.hc"},
                    "present_count": {"$sum": "$events.p"}, "late_count": {"$sum": "$events.l"}}},
        {"$addFields": {"day": {"$dateFromString": {"dateString": "$_id", "format": "%Y-%m-%d", "timezone": "UTC"}}}},
        # Gap filling: exactly one document per calendar day in [from, to].
        {"$densify": {"field": "day", "range": {"step": 1, "unit": "day", "bounds": [lower, upper]}}},
        # Headcount on a day = everyone who has joined by then = running total of "joined".
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "headcount": {"$sum": "$joined", "window": {"documents": ["unbounded", "current"]}}}}},
        {"$addFields": {
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
        }},
        {"$addFields": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            round_half_up({"$divide": [{"$toDecimal": "$present_count"}, "$headcount"]}, 4),
            None,
        ]}}},
        # $avg ignores nulls and returns null if the window has no number. Rows before `from` don't exist
        # in this pipeline, so the first rows naturally get a shorter window.
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "moving_avg_7d": {"$avg": "$attendance_rate", "window": {"documents": [-6, 0]}}}}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": 1,
            "headcount": 1,
            "present_count": to_double(round_half_up("$present_count", 2)),
            "late_count": 1,
            "attendance_rate": to_double("$attendance_rate"),
            "moving_avg_7d": to_double(round_half_up("$moving_avg_7d", 4)),
        }},
    ]
    return "employees", pipeline


def trend_range(from_: str, to: str) -> tuple[date, date]:
    first = parse_date(from_, ("query", "from"))
    last = parse_date(to, ("query", "to"))
    if last < first:
        reject("to must not be before from", ("query", "to"))
    if (last - first).days + 1 > MAX_TREND_DAYS:
        reject(f"the range may not exceed {MAX_TREND_DAYS} days", ("query", "to"))
    return first, last


def monthly_working_days(emp: dict, month: str) -> int:
    start, end = month_bounds(month)
    first = max(date.fromisoformat(start), date.fromisoformat(emp["joined_on"]))  # R7 mid-month joiners
    return count_weekdays(first, date.fromisoformat(end))


# --------------------------------------------------------------------------- #
# 8. Analytics endpoints
# --------------------------------------------------------------------------- #
@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthParam):
    emp = get_employee_or_404(emp_code)
    working_days = monthly_working_days(emp, month)
    collection, pipeline = employee_monthly_pipeline(emp_code, month, working_days)
    rows = list(db[collection].aggregate(pipeline))
    # $group emits nothing when there are no records: that month is all zeros.
    stats = rows[0] if rows else {
        "present_days": 0.0, "leave_days": 0, "late_count": 0, "total_late_minutes": 0,
        "total_overtime_minutes": 0, "attendance_pct": 0.0 if working_days > 0 else None,
    }
    return {"emp_code": emp_code, "month": month, "working_days": working_days, **stats}


@app.get("/analytics/departments/summary")
def department_summary(month: MonthParam, department: Optional[str] = None):
    collection, pipeline = department_summary_pipeline(month, department)
    return {"month": month, "items": list(db[collection].aggregate(pipeline))}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(month: MonthParam, limit: Annotated[int, Query(ge=1, le=50)] = 10,
                     department: Optional[str] = None):
    collection, pipeline = late_leaderboard_pipeline(month, limit, department)
    return {"month": month, "items": list(db[collection].aggregate(pipeline))}


@app.get("/analytics/departments/{department}/trend")
def department_trend(department: str, from_: Annotated[str, Query(alias="from")],
                     to: Annotated[str, Query()]):
    first, last = trend_range(from_, to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise HTTPException(404, f"department {department} not found")
    collection, pipeline = department_trend_pipeline(department, first, last)
    return {"department": department, "items": list(db[collection].aggregate(pipeline))}


# --------------------------------------------------------------------------- #
# 9. Explain endpoint
# --------------------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard",
                        "department_trend"]


def _require(value: Optional[str], name: str) -> str:
    if value is None:
        reject(f"{name} is required for this endpoint", ("query", name))
    return value


@app.get("/admin/explain/{endpoint}")
def explain(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Annotated[Optional[str], Query(pattern=r"^\d{4}-(0[1-9]|1[0-2])$")] = None,
    department: Optional[str] = None,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Annotated[Optional[str], Query(alias="from")] = None,
    to: Optional[str] = None,
    page: PageParam = 1,
    page_size: PageSizeParam = 20,
):
    if endpoint == "attendance_list":
        query = attendance_list_query(emp_code, date_from, date_to, status)
        collection = "attendance_logs"
        command = {"find": collection, "filter": query, "projection": {"_id": 0},
                   "sort": dict(ATTENDANCE_SORT), "skip": (page - 1) * page_size, "limit": page_size}
    else:
        if endpoint == "employee_monthly":
            emp = get_employee_or_404(_require(emp_code, "emp_code"))
            month = _require(month, "month")
            collection, pipeline = employee_monthly_pipeline(emp["emp_code"], month, monthly_working_days(emp, month))
        elif endpoint == "department_summary":
            collection, pipeline = department_summary_pipeline(_require(month, "month"), department)
        elif endpoint == "late_leaderboard":
            collection, pipeline = late_leaderboard_pipeline(_require(month, "month"), limit, department)
        else:  # department_trend
            first, last = trend_range(_require(from_, "from"), _require(to, "to"))
            collection, pipeline = department_trend_pipeline(_require(department, "department"), first, last)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    result = db.command({"explain": command, "verbosity": "executionStats"})
    # Raw explain output, converted from BSON types (Int64, Timestamp, ...) to plain JSON.
    return {"endpoint": endpoint, "collection": collection,
            "explain": json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))}
