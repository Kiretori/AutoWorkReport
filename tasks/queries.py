from datetime import timedelta, date
from typing import Any, List, Tuple, Sequence
from sqlalchemy import Row, select, func
from sqlalchemy.orm import Session, joinedload
from database.models import DateDimension, DimEmployee, DailyAttendance
from prefect import task
from prefect.logging import get_run_logger, disable_run_logger
from database.db import get_engine


@task
def fetch_employees_per_date(
    target_date_id: int,
) -> Sequence[Row[Tuple[DimEmployee, DailyAttendance, Any]]] | None:
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    with Session(engine) as session:
        work_duration = DailyAttendance.check_out_hour - DailyAttendance.check_in_hour

        stmt = (
            select(DimEmployee, DailyAttendance, work_duration)
            .join(DailyAttendance, DimEmployee.id == DailyAttendance.id_employee)
            .join(DateDimension, DailyAttendance.date_id == DateDimension.date_id)
            .where(~DateDimension.est_ferie)  # Not holiday
            .where(DateDimension.date_id == target_date_id)
            .options(joinedload(DailyAttendance.date_table))
        )
        employees = session.execute(stmt).all()

    return employees


def fetch_absent_employees(
    employees_data: Sequence[Row[Tuple[DimEmployee, DailyAttendance, Any]]],
) -> List[DimEmployee]:
    absent_employees = []

    for e, d, _ in employees_data:
        # 3rd (2nd index) column is 'present'
        if not d.present:
            absent_employees.append(e)
    return absent_employees


def fetch_employees_under_working(
    employees_data: Sequence[Row[Tuple[DimEmployee, DailyAttendance, Any]]],
    under_work_threshold: float,
) -> List[Tuple[DimEmployee, Any]]:
    under_work_hours_timedelta = timedelta(hours=under_work_threshold)
    filtered_employees = []

    for emp, _, dur in employees_data:
        if dur and dur < under_work_hours_timedelta:
            filtered_employees.append((emp, dur))

    return filtered_employees


def total_employee_count() -> int:
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    with Session(engine) as session:
        stmt = select(func.count()).select_from(DimEmployee)
        employee_count = session.execute(stmt).scalar_one()

    return employee_count


def get_last_workweek() -> Tuple[date, date]:
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_friday = last_monday + timedelta(days=4)
    return last_monday, last_friday


def fetch_employee_weekly_data(
    start_date: date, end_date: date
) -> Sequence[Row[Tuple[DimEmployee, DailyAttendance, Any]]]:
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    with Session(engine) as session:
        work_duration = DailyAttendance.check_out_hour - DailyAttendance.check_in_hour

        stmt = (
            select(DimEmployee, DailyAttendance, work_duration)
            .join(DailyAttendance, DimEmployee.id == DailyAttendance.id_employee)
            .join(DateDimension, DailyAttendance.date_id == DateDimension.date_id)
            .where(~DateDimension.est_ferie)
            .where(DateDimension.date_literale >= start_date)
            .where(DateDimension.date_literale <= end_date)
            .options(joinedload(DailyAttendance.date_table))
        )
        employees = session.execute(stmt).all()

    return employees


# todo: remove later
def test_weekly():
    start_date, end_date = date(2025, 7, 7), date(2025, 7, 11)
    return fetch_employee_weekly_data(start_date, end_date)


if __name__ == "__main__":
    with disable_run_logger():
        employees = test_weekly()
        absent = fetch_absent_employees(employees)
        for employee in absent:
            print(f"{employee.first_name} {employee.last_name}")
