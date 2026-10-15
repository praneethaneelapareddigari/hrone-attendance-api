"""End-to-end tests against a real MongoDB.

    MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_test pytest -q

The database named in MONGO_DB is DROPPED, so it must end with "_test".
Expected analytics values below were worked out by hand from the rules R1-R10.
"""
import os
import threading
from datetime import datetime, timedelta, timezone

import pytest

if not os.environ.get("MONGO_DB", "").endswith("_test"):
    pytest.skip("set MONGO_DB to a database name ending in _test", allow_module_level=True)

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app, db  # noqa: E402

IST = timezone(timedelta(hours=5, minutes=30))
UTC = timezone.utc


def ms(iso_ist: str) -> int:
    """'2026-07-06T09:40:01' (IST) -> epoch ms."""
    return int(datetime.fromisoformat(iso_ist).replace(tzinfo=IST).timestamp() * 1000)


def utc(iso_ist: str) -> datetime:
    return datetime.fromisoformat(iso_ist).replace(tzinfo=IST).astimezone(UTC)


def emp(code, dept, joined, start="09:30", end="18:30"):
    return {"emp_code": code, "name": f"Name {code}", "email": f"{code.lower()}@example.com", "department": dept,
            "shift_start": start, "shift_end": end, "joined_on": joined, "created_at": utc("2026-01-01T10:00:00")}


def rec(code, day, status, late=0, ot=0, wh=None, half=False, legacy=False):
    doc = {"emp_code": code, "date": day, "status": status,
           "punch_in": utc(f"{day}T09:30:00") if status in ("PRESENT", "WFH", "ON_DUTY") else None,
           "punch_out": None, "work_hours": wh, "late_minutes": late, "overtime_minutes": ot,
           "half_day": half, "history": []}
    if wh is not None:
        doc["punch_out"] = doc["punch_in"] + timedelta(hours=wh)
    if legacy:  # older seeded records omit these
        for k in ("half_day", "history", "late_minutes"):
            doc.pop(k)
    return doc


ANALYTICS_EMPLOYEES = [
    emp("EMP1001", "Engineering", "2026-01-01"),
    emp("EMP1002", "Engineering", "2026-07-15"),   # mid-month joiner
    emp("EMP1003", "Engineering", "2026-01-01"),   # no logs at all
    emp("EMP1004", "Legal", "2026-08-10"),         # joins after July
    emp("EMP2001", "Sales", "2026-01-01"),
    emp("EMP2002", "Sales", "2026-01-01"),
    emp("EMP2003", "Sales", "2026-01-01"),
    emp("EMP3001", "Ops", "2026-01-01"),
]
# July 2026: the 1st is a Wednesday; weekends are 4/5, 11/12, 18/19, 25/26.
ANALYTICS_LOGS = [
    rec("EMP1001", "2026-06-30", "PRESENT", late=99, wh=8.0),         # previous month, excluded
    rec("EMP1001", "2026-07-01", "PRESENT", late=15, wh=8.0),
    rec("EMP1001", "2026-07-02", "WFH", wh=4.0, half=True),
    rec("EMP1001", "2026-07-03", "ABSENT"),
    rec("EMP1001", "2026-07-04", "PRESENT", late=20, ot=40, wh=9.5),  # Saturday
    rec("EMP1001", "2026-07-06", "LEAVE"),
    rec("EMP1001", "2026-07-07", "ON_DUTY", wh=9.0, legacy=True),
    rec("EMP1002", "2026-07-15", "PRESENT", late=30, wh=9.0),
    rec("EMP1002", "2026-07-16", "PRESENT", late=30, wh=7.0),
    rec("EMP2001", "2026-07-08", "PRESENT", late=35, wh=8.33),
    rec("EMP2002", "2026-07-09", "PRESENT", late=10, wh=8.34),
    rec("EMP2003", "2026-07-10", "ON_DUTY", late=60),                 # still open, no work_hours
    rec("EMP2003", "2026-07-13", "WFH", wh=3.0, half=True),
    rec("EMP3001", "2026-07-14", "PRESENT", wh=8.33),
    rec("EMP3001", "2026-07-15", "PRESENT", wh=8.34),                 # avg 8.335 -> 8.34 (half-up)
    rec("EMP9999", "2026-07-08", "PRESENT", late=500, wh=8.0),        # employee does not exist
]


@pytest.fixture(scope="module")
def client():
    db.client.drop_database(db.name)
    with TestClient(app) as c:
        db.employees.insert_many([dict(e) for e in ANALYTICS_EMPLOYEES])
        db.attendance_logs.insert_many([dict(r) for r in ANALYTICS_LOGS])
        yield c


def parallel(n, fn):
    results, barrier = [], threading.Barrier(n)

    def run(i):
        barrier.wait()
        results.append(fn(i))

    threads = [threading.Thread(target=run, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


# ------------------------------------------------------------------ system / employees
def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_create_employee_and_validation(client):
    body = {"emp_code": "EMP5001", "name": "A", "email": "a@x.io", "department": "QA", "joined_on": "2026-07-01"}
    r = client.post("/employees", json=body)
    assert r.status_code == 201
    out = r.json()
    assert isinstance(out["created_at"], int) and "_id" not in out and out["shift_start"] == "09:30"
    assert client.post("/employees", json=body).status_code == 409
    for bad in ({"emp_code": "E1"}, {"email": "nope"}, {"joined_on": "2026-13-01"},
                {"shift_start": "10:00", "shift_end": "10:00"}, {"shift_start": "24:00"}, {"name": ""}):
        assert client.post("/employees", json={**body, "emp_code": "EMP5002", **bad}).status_code == 422, bad


def test_concurrent_employee_create(client):
    body = {"emp_code": "EMP5100", "name": "A", "email": "a@x.io", "department": "QA", "joined_on": "2026-07-01"}
    codes = parallel(8, lambda _: client.post("/employees", json=body).status_code)
    assert sorted(codes) == [201] + [409] * 7


def test_list_employees(client):
    r = client.get("/employees", params={"department": "Engineering", "page_size": 2})
    j = r.json()
    assert j["total"] == 3 and [e["emp_code"] for e in j["items"]] == ["EMP1001", "EMP1002"]
    j = client.get("/employees", params={"department": "Engineering", "page": 2, "page_size": 2}).json()
    assert [e["emp_code"] for e in j["items"]] == ["EMP1003"]
    assert client.get("/employees", params={"page": 0}).status_code == 422
    assert client.get("/employees", params={"page_size": 101}).status_code == 422


# ------------------------------------------------------------------ punch-in / punch-out
@pytest.fixture(scope="module")
def puncher(client):
    db.employees.insert_many([emp("EMP6001", "Floor", "2026-01-01"),
                              emp("EMP6002", "Floor", "2026-01-01", "22:00", "06:00"),
                              emp("EMP6003", "Floor", "2026-01-01")])
    return client


def test_punch_in_rules(puncher):
    c = puncher
    assert c.post("/attendance/punch-in", json={"emp_code": "EMP0000"}).status_code == 404
    for bad in (1783312500, 1.7833125e12, "1783312500000", True):
        assert c.post("/attendance/punch-in", json={"emp_code": "EMP6001", "punched_at": bad}).status_code == 422
    assert c.post("/attendance/punch-in", json={"emp_code": "EMP6001", "status": "ABSENT"}).status_code == 422

    r = c.post("/attendance/punch-in", json={"emp_code": "EMP6001", "punched_at": ms("2026-08-03T09:40:01") + 900})
    assert r.status_code == 201
    j = r.json()
    assert j["late_minutes"] == 10 and j["date"] == "2026-08-03"
    assert j["punch_in"] == ms("2026-08-03T09:40:01")  # truncated to the second
    assert j["punch_out"] is None and j["work_hours"] is None and j["history"] == [] and "id" not in j

    for t, late in (("2026-08-04T09:40:00", 0), ("2026-08-05T10:15:59", 45)):
        assert c.post("/attendance/punch-in", json={"emp_code": "EMP6003", "punched_at": ms(t)}).json()["late_minutes"] == late


def test_concurrent_punch_in(puncher):
    body = {"emp_code": "EMP6001", "punched_at": ms("2026-08-10T09:00:00")}
    codes = parallel(10, lambda _: puncher.post("/attendance/punch-in", json=body).status_code)
    assert sorted(codes) == [201] + [409] * 9


def test_punch_out_and_half_day(puncher):
    c = puncher
    out = c.post("/attendance/punch-out", json={"emp_code": "EMP6001", "punched_at": ms("2026-08-03T14:10:00")})
    assert out.status_code == 200
    j = out.json()
    assert j["work_hours"] == 4.5 and j["half_day"] is False and j["overtime_minutes"] == 0  # 4.4997 -> 4.50
    assert c.post("/attendance/punch-out", json={"emp_code": "EMP6001",
                                                 "punched_at": ms("2026-08-03T15:00:00")}).status_code == 409
    # 4h 29m 41s = 4.4947 -> 4.49 -> half day
    j = c.post("/attendance/punch-out", json={"emp_code": "EMP6003", "punched_at": ms("2026-08-04T14:09:41")}).json()
    assert j["work_hours"] == 4.49 and j["half_day"] is True
    # Overtime: 30 minutes counts, 29 does not
    j = c.post("/attendance/punch-out", json={"emp_code": "EMP6003", "punched_at": ms("2026-08-05T19:00:59")}).json()
    assert j["overtime_minutes"] == 30
    assert c.post("/attendance/punch-out", json={"emp_code": "EMP6002"}).status_code == 404


def test_overnight_shift(puncher):
    c = puncher
    j = c.post("/attendance/punch-in", json={"emp_code": "EMP6002", "punched_at": ms("2026-09-07T21:55:00")}).json()
    assert j["date"] == "2026-09-07" and j["late_minutes"] == 0
    j = c.post("/attendance/punch-out", json={"emp_code": "EMP6002", "punched_at": ms("2026-09-08T06:40:00")}).json()
    assert j["date"] == "2026-09-07" and j["work_hours"] == 8.75 and j["overtime_minutes"] == 40
    # A punch-in at 05:30 belongs to the shift that started the previous evening (very late).
    j = c.post("/attendance/punch-in", json={"emp_code": "EMP6002", "punched_at": ms("2026-09-10T05:30:00")}).json()
    assert j["date"] == "2026-09-09" and j["late_minutes"] == 450


def test_punch_out_limits_and_race(puncher):
    c = puncher
    c.post("/attendance/punch-in", json={"emp_code": "EMP6003", "punched_at": ms("2026-08-20T09:00:00")})
    assert c.post("/attendance/punch-out", json={"emp_code": "EMP6003",
                                                 "punched_at": ms("2026-08-21T09:00:01")}).status_code == 422
    body = {"emp_code": "EMP6003", "punched_at": ms("2026-08-20T18:00:00")}
    codes = parallel(10, lambda _: c.post("/attendance/punch-out", json=body).status_code)
    assert sorted(codes) == [200] + [409] * 9


# ------------------------------------------------------------------ regularize
def test_regularize(puncher):
    c = puncher
    c.post("/attendance/punch-in", json={"emp_code": "EMP6003", "punched_at": ms("2026-08-24T10:05:00")})
    c.post("/attendance/punch-out", json={"emp_code": "EMP6003", "punched_at": ms("2026-08-24T18:30:00")})
    url = "/attendance/EMP6003/2026-08-24"
    who = {"reason": "biometric glitch", "regularized_by": "hr.admin"}

    r = c.patch(url, json={**who, "punch_in": ms("2026-08-24T09:28:00"), "work_hours": 99})
    assert r.status_code == 200
    j = r.json()
    assert j["late_minutes"] == 0 and j["work_hours"] == 9.03
    (h,) = j["history"]
    assert h["by"] == "hr.admin" and isinstance(h["at"], int)
    assert h["changes"] == {"punch_in": {"from": ms("2026-08-24T10:05:00"), "to": ms("2026-08-24T09:28:00")},
                            "late_minutes": {"from": 35, "to": 0}, "work_hours": {"from": 8.42, "to": 9.03}}

    assert c.patch(url, json={**who, "punch_in": ms("2026-08-24T09:28:00")}).status_code == 422  # no-op
    assert c.patch(url, json={**who, "status": "ABSENT", "punch_in": ms("2026-08-24T09:00:00")}).status_code == 422
    assert c.patch(url, json={**who, "punch_in": ms("2026-08-25T09:00:00")}).status_code == 422  # other day
    assert c.patch(url, json={**who, "punch_out": ms("2026-08-24T09:00:00")}).status_code == 422  # before in
    assert c.patch(url, json={**who, "reason": "x"}).status_code == 422
    assert c.patch("/attendance/EMP6003/2026-01-01", json={**who, "status": "LEAVE"}).status_code == 404
    assert c.patch("/attendance/EMP0000/2026-08-24", json={**who, "status": "LEAVE"}).status_code == 404
    assert c.patch("/attendance/EMP6003/24-08-2026", json={**who, "status": "LEAVE"}).status_code == 422

    j = c.patch(url, json={**who, "status": "ABSENT"}).json()
    assert j["punch_in"] is None and j["punch_out"] is None and j["work_hours"] is None and len(j["history"]) == 2
    assert c.patch(url, json={**who, "status": "PRESENT"}).status_code == 422  # needs a punch_in now


def test_concurrent_regularize_keeps_history(puncher):
    c = puncher
    c.post("/attendance/punch-in", json={"emp_code": "EMP6003", "punched_at": ms("2026-08-26T09:30:00")})
    url = "/attendance/EMP6003/2026-08-26"
    outs = parallel(8, lambda i: c.patch(url, json={"reason": f"fix number {i}", "regularized_by": "hr",
                                                    "punch_out": ms("2026-08-26T17:00:00") + i * 60_000}).status_code)
    assert set(outs) <= {200, 409} and 200 in outs
    stored = db.attendance_logs.find_one({"emp_code": "EMP6003", "date": "2026-08-26"})
    assert len(stored["history"]) == outs.count(200)


def test_list_attendance(client):
    j = client.get("/attendance", params={"date_from": "2026-07-01", "date_to": "2026-07-31", "page_size": 100}).json()
    keys = [(r["date"], r["emp_code"]) for r in j["items"]]
    assert keys == sorted(keys, key=lambda k: (-int(k[0].replace("-", "")), k[1]))
    assert j["total"] == len(j["items"]) == 15
    leg = [r for r in j["items"] if r["emp_code"] == "EMP1001" and r["date"] == "2026-07-07"][0]
    assert leg["half_day"] is False and leg["history"] == [] and leg["late_minutes"] == 0
    assert client.get("/attendance", params={"status": "LEAVE"}).json()["total"] == 1
    assert client.get("/attendance", params={"date_from": "2026-07-02", "date_to": "2026-07-01"}).status_code == 422
    assert client.get("/attendance", params={"status": "NOPE"}).status_code == 422


# ------------------------------------------------------------------ analytics
def test_employee_monthly(client):
    j = client.get("/analytics/employees/EMP1001/monthly", params={"month": "2026-07"}).json()
    assert j == {"emp_code": "EMP1001", "month": "2026-07", "working_days": 23, "present_days": 2.5,
                 "leave_days": 1, "late_count": 2, "total_late_minutes": 35, "total_overtime_minutes": 40,
                 "attendance_pct": 10.87}
    j = client.get("/analytics/employees/EMP1002/monthly", params={"month": "2026-07"}).json()
    assert j["working_days"] == 13 and j["present_days"] == 2.0 and j["attendance_pct"] == 15.38
    j = client.get("/analytics/employees/EMP1003/monthly", params={"month": "2026-07"}).json()
    assert j["present_days"] == 0 and j["attendance_pct"] == 0.0
    j = client.get("/analytics/employees/EMP1004/monthly", params={"month": "2026-07"}).json()
    assert j["working_days"] == 0 and j["attendance_pct"] is None
    assert client.get("/analytics/employees/EMP0000/monthly", params={"month": "2026-07"}).status_code == 404
    assert client.get("/analytics/employees/EMP1001/monthly", params={"month": "2026-13"}).status_code == 422


def test_department_summary(client):
    items = client.get("/analytics/departments/summary", params={"month": "2026-07"}).json()["items"]
    by = {i["department"]: i for i in items}
    assert [i["department"] for i in items] == sorted(by) and "Legal" not in by
    assert by["Engineering"] == {"department": "Engineering", "headcount": 3, "present_days": 4.5,
                                 "avg_work_hours": 7.75, "late_count": 4, "total_late_minutes": 95,
                                 "leave_count": 1, "on_duty_count": 1}
    assert by["Sales"] == {"department": "Sales", "headcount": 3, "present_days": 3.5, "avg_work_hours": 6.56,
                           "late_count": 3, "total_late_minutes": 105, "leave_count": 0, "on_duty_count": 1}
    assert by["Ops"]["avg_work_hours"] == 8.34
    one = client.get("/analytics/departments/summary", params={"month": "2026-07", "department": "Sales"}).json()
    assert [i["department"] for i in one["items"]] == ["Sales"]


def test_late_leaderboard(client):
    def board(**p):
        return [(i["rank"], i["emp_code"]) for i in
                client.get("/analytics/leaderboard/late", params={"month": "2026-07", **p}).json()["items"]]
    assert board() == [(1, "EMP1002"), (1, "EMP2003"), (3, "EMP1001"), (3, "EMP2001"), (5, "EMP2002")]
    assert board(limit=1) == [(1, "EMP1002"), (1, "EMP2003")]
    assert board(limit=2) == [(1, "EMP1002"), (1, "EMP2003")]
    assert board(limit=3) == [(1, "EMP1002"), (1, "EMP2003"), (3, "EMP1001"), (3, "EMP2001")]
    assert board(department="Sales", limit=2) == [(1, "EMP2003"), (2, "EMP2001")]
    row = client.get("/analytics/leaderboard/late", params={"month": "2026-07"}).json()["items"][2]
    assert row == {"rank": 3, "emp_code": "EMP1001", "name": "Name EMP1001", "department": "Engineering",
                   "total_late_minutes": 35, "late_count": 2}
    assert client.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 51}).status_code == 422


def test_department_trend(client):
    def trend(dept, a, b):
        return client.get(f"/analytics/departments/{dept}/trend", params={"from": a, "to": b})

    items = trend("Engineering", "2026-07-01", "2026-07-10").json()["items"]
    assert [i["date"] for i in items] == [f"2026-07-{d:02d}" for d in range(1, 11)]
    assert [i["headcount"] for i in items] == [2] * 10
    assert [i["present_count"] for i in items] == [1, 0.5, 0, 1, 0, 0, 1, 0, 0, 0]
    assert [i["late_count"] for i in items] == [1, 0, 0, 1, 0, 0, 0, 0, 0, 0]
    assert [i["attendance_rate"] for i in items] == [0.5, 0.25, 0.0, None, None, 0.0, 0.5, 0.0, 0.0, 0.0]
    assert [i["moving_avg_7d"] for i in items] == [0.5, 0.375, 0.25, 0.25, 0.25, 0.1875, 0.25, 0.15, 0.1, 0.1]
    assert [i["is_working_day"] for i in items] == [True, True, True, False, False, True, True, True, True, True]

    items = trend("Engineering", "2026-07-13", "2026-07-17").json()["items"]
    assert [i["headcount"] for i in items] == [2, 2, 3, 3, 3]
    assert [i["attendance_rate"] for i in items] == [0.0, 0.0, 0.3333, 0.3333, 0.0]
    assert [i["moving_avg_7d"] for i in items] == [0.0, 0.0, 0.1111, 0.1667, 0.1333]

    items = trend("Legal", "2026-08-08", "2026-08-11").json()["items"]
    assert [(i["headcount"], i["attendance_rate"]) for i in items] == [(0, None), (0, None), (1, 0.0), (1, 0.0)]
    assert [i["moving_avg_7d"] for i in items] == [None, None, 0.0, 0.0]

    assert len(trend("Engineering", "2026-07-01", "2026-09-30").json()["items"]) == 92
    assert trend("Engineering", "2026-07-01", "2026-10-01").status_code == 422
    assert trend("Engineering", "2026-07-10", "2026-07-01").status_code == 422
    assert trend("Nobody", "2026-07-01", "2026-07-02").status_code == 404


def test_explain_uses_indexes(client):
    cases = {
        "attendance_list": {"date_from": "2026-07-01"},
        "employee_monthly": {"emp_code": "EMP1001", "month": "2026-07"},
        "department_summary": {"month": "2026-07"},
        "late_leaderboard": {"month": "2026-07"},
        "department_trend": {"department": "Engineering", "from": "2026-07-01", "to": "2026-07-10"},
    }
    for name, params in cases.items():
        r = client.get(f"/admin/explain/{name}", params=params)
        assert r.status_code == 200, (name, r.text)
        text = str(r.json()["explain"])
        assert "IXSCAN" in text and "COLLSCAN" not in text, name
    assert client.get("/admin/explain/late_leaderboard").status_code == 422
    assert client.get("/admin/explain/nope").status_code == 422
