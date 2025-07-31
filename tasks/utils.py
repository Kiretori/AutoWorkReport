import csv
from datetime import timedelta, date
import os
from typing import Any, List, Tuple, Sequence
from sqlalchemy import Integer, Row, select, func
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


def fetch_weekly_data(
    start_date: date, end_date: date
) -> Sequence[Row[Tuple[date, str, bool, int, int, Any]]]:
    """
    return date | day name | is holiday | absence count | total employees | absence percentage
    """

    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    with Session(engine) as session:
        stmt = (
            select(
                DateDimension.date_literale,
                DateDimension.nom_jour,
                DateDimension.est_ferie,
                func.sum((~DailyAttendance.present).cast(Integer)).label("absence"),
                func.count(DimEmployee.id).label("employee_count"),
                (
                    func.round(
                        func.sum((~DailyAttendance.present).cast(Integer))
                        * 100.0
                        / func.count(DimEmployee.id),
                        2,
                    )
                ).label("absence_percentage"),
            )
            .join(DailyAttendance, DateDimension.date_id == DailyAttendance.date_id)
            .join(DimEmployee, DimEmployee.id == DailyAttendance.id_employee)
            .where(DateDimension.date_literale.between(start_date, end_date))
            .group_by(
                DateDimension.date_literale,
                DateDimension.nom_jour,
                DateDimension.est_ferie,
            )
            .order_by(DateDimension.date_literale.asc())
        )
        employees = session.execute(stmt).all()

    return employees


@task
def generate_daily_csv(
    daily_data: Sequence[Row[Tuple[DimEmployee, Any, bool]]], filename: str
):
    def format_timedelta(td: timedelta) -> str:
        total_minutes = td.total_seconds() // 60
        hours = int(total_minutes // 60)
        minutes = int(total_minutes % 60)
        return f"{hours}h {minutes}m"

    os.makedirs("data/daily_reports/", exist_ok=True)
    with open(
        f"data/daily_reports/{filename}", mode="w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.writer(file)

        # Header
        writer.writerow(["Matricule", "Nom", "Prénom", "Présent", "Durée de travail"])

        for emp, daily, dur in daily_data:
            matricule = emp.matricule
            first_name = emp.first_name
            last_name = emp.last_name
            present = "OUI" if daily.present else "NON"
            work_duration = format_timedelta(dur) if present == "OUI" else "0"

            writer.writerow([matricule, last_name, first_name, present, work_duration])

    return f"data/daily_reports/{filename}"


@task
def generate_weekly_csv(
    weekly_data: Sequence[Row[Tuple[date, str, bool, int, int, Any]]], filename: str
):
    def format_timedelta(td: timedelta) -> str:
        total_minutes = td.total_seconds() // 60
        hours = int(total_minutes // 60)
        minutes = int(total_minutes % 60)
        return f"{hours}h {minutes}m"

    os.makedirs("data/weekly_reports/", exist_ok=True)
    with open(
        f"data/weekly_reports/{filename}", mode="w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.writer(file)

        # Header
        writer.writerow(
            ["Date", "Jour", "Férié", "Nombre d'absences", "Pourcentage d'absence"]
        )

        for (
            date_literal,
            day_name,
            is_holiday,
            abs_count,
            _,
            abs_percentage,
        ) in weekly_data:
            if is_holiday:
                ferie = "OUI"
                abs_count = 0
                abs_percentage = 0
            else:
                ferie = "OUI"

            writer.writerow([date_literal, day_name, ferie, abs_count, abs_percentage])

    return f"data/weekly_reports/{filename}"


# todo: remove later
def test_weekly():
    start_date, end_date = date(2025, 7, 7), date(2025, 7, 11)
    return fetch_weekly_data(start_date, end_date)


if __name__ == "__main__":
    with disable_run_logger():
        employees = test_weekly()
        generate_weekly_csv.fn(employees, "weekly_test.csv")
