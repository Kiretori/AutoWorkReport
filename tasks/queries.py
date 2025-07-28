from datetime import timedelta
from typing import Any, List, Tuple, Sequence
from sqlalchemy import Row, create_engine, select, func
from sqlalchemy.orm import Session
from database.models import DimEmployee, DailyAttendance
from prefect import task
from prefect.logging import get_run_logger
from dotenv import load_dotenv
from tasks import DB_URL

load_dotenv()

_engine = None


# Engine singleton
def get_engine(db_url):
    global _engine
    if _engine is None:
        _engine = create_engine(db_url)
    return _engine


@task
def fetch_employees_per_date(
    target_date_id: int,
) -> Sequence[Row[Tuple[DimEmployee, Any, bool]]] | None:
    logger = get_run_logger()
    if not DB_URL:
        logger.error("Database URL is empty.")
        exit(1)
    try:
        engine = get_engine(DB_URL)
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    with Session(engine) as session:
        work_duration = DailyAttendance.check_out_hour - DailyAttendance.check_in_hour

        employees = session.execute(
            select(
                DimEmployee,
                work_duration.label("work_duration"),
                DailyAttendance.present,
            )
            .join(DailyAttendance, DimEmployee.id == DailyAttendance.id_employee)
            .where(DailyAttendance.date_id == target_date_id)
        ).all()

    return employees


def fetch_absent_employees(
    employees_data: Sequence[Row[Tuple[DimEmployee, Any, bool]]],
) -> List[DimEmployee]:
    absent_employees = []

    for row in employees_data:
        # 3rd (2nd index) column is 'present'
        if not row[2]:
            absent_employees.append(row[0])
    return absent_employees


def fetch_employees_under_working(
    employees_data: Sequence[Row[Tuple[DimEmployee, Any, bool]]],
    under_work_threshold: float,
) -> List[Tuple[DimEmployee, Any]]:
    under_work_hours_timedelta = timedelta(hours=under_work_threshold)
    filtered_employees = []

    for row in employees_data:
        if row[1] and row[1] < under_work_hours_timedelta:
            filtered_employees.append((row[0], row[1]))

    return filtered_employees


def total_employee_count() -> int:
    logger = get_run_logger()
    if not DB_URL:
        logger.error("Database URL is empty.")
        exit(1)
    try:
        engine = get_engine(DB_URL)
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    with Session(engine) as session:
        stmt = select(func.count()).select_from(DimEmployee)
        employee_count = session.execute(stmt).scalar_one()

    return employee_count


if __name__ == "__main__":
    ...
