"""Fills MONGO_DB with ~100,000 attendance records (500 employees x 200 days) for speed / explain checks.

    MONGO_URI=mongodb://localhost:27017 MONGO_DB=attendance_load python scripts/load_100k.py

WARNING: deletes everything in the two collections of that database first.
"""
import os
import random
from datetime import date, datetime, timedelta, timezone

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
IST = timezone(timedelta(hours=5, minutes=30))
db = MongoClient(os.environ["MONGO_URI"], tz_aware=True)[os.environ["MONGO_DB"]]
rng = random.Random(42)
departments = ["Engineering", "Sales", "Support", "Finance", "HR", "Ops"]

emps = []
for i in range(500):
    emps.append({"emp_code": f"EMP{i + 1:04d}", "name": f"Employee {i + 1}", "email": f"e{i + 1}@example.com",
                 "department": departments[i % len(departments)], "shift_start": "09:30", "shift_end": "18:30",
                 "joined_on": (date(2026, 1, 1) + timedelta(days=rng.randint(0, 200))).isoformat(),
                 "created_at": datetime.now(timezone.utc)})

docs, first = [], date(2026, 1, 1)
for e in emps:
    for d in range(200):
        day = first + timedelta(days=d)
        status = rng.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [70, 15, 5, 5, 5])[0]
        doc = {"emp_code": e["emp_code"], "date": day.isoformat(), "status": status, "punch_in": None,
               "punch_out": None, "work_hours": None, "late_minutes": 0, "overtime_minutes": 0,
               "half_day": False, "history": []}
        if status in ("PRESENT", "WFH", "ON_DUTY"):
            pin = datetime(day.year, day.month, day.day, 9, 30, tzinfo=IST) + timedelta(seconds=rng.randint(-1800, 3600))
            secs = rng.randint(3 * 3600, 10 * 3600)
            late = int((pin - datetime(day.year, day.month, day.day, 9, 30, tzinfo=IST)).total_seconds())
            doc.update(punch_in=pin, punch_out=pin + timedelta(seconds=secs), work_hours=round(secs / 3600, 2),
                       late_minutes=late // 60 if late > 600 else 0, half_day=secs / 3600 < 4.5)
        docs.append(doc)

db.employees.delete_many({})
db.attendance_logs.delete_many({})
db.employees.insert_many(emps)
for i in range(0, len(docs), 10_000):
    db.attendance_logs.insert_many(docs[i:i + 10_000])
print(f"inserted {len(emps)} employees and {len(docs)} attendance records into {db.name}")
