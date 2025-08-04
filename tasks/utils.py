import csv
from datetime import timedelta, date
import os
from typing import Any, List, Tuple, Sequence
from sqlalchemy import Integer, Row, select, func
from sqlalchemy.orm import Session, joinedload
from database.models import (
    DateDimension,
    DimEmployee,
    DailyAttendance,
    DimService,
)
from prefect import task
from prefect.logging import get_run_logger, disable_run_logger
from database.db import get_engine
import xlsxwriter


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

    work_duration = DailyAttendance.check_out_hour - DailyAttendance.check_in_hour

    stmt = (
        select(DimEmployee, DailyAttendance, work_duration)
        .join(DailyAttendance, DimEmployee.id == DailyAttendance.id_employee)
        .join(DateDimension, DailyAttendance.date_id == DateDimension.date_id)
        .where(~DateDimension.est_ferie)  # Not holiday
        .where(DateDimension.date_id == target_date_id)
        .options(joinedload(DailyAttendance.date_table))
    )

    with Session(engine) as session:
        employees = session.execute(stmt).all()

    return employees


@task
def fetch_absent_employees(
    employees_data: Sequence[Row[Tuple[DimEmployee, DailyAttendance, Any]]],
) -> List[DimEmployee]:
    absent_employees = []

    for e, d, _ in employees_data:
        # 3rd (2nd index) column is 'present'
        if not d.present:
            absent_employees.append(e)
    return absent_employees


@task
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
        employee_count = session.execute(
            select(func.count()).select_from(DimEmployee)
        ).scalar_one()

    return employee_count


@task
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

    with Session(engine) as session:
        employees = session.execute(stmt).all()

    return employees


@task
def fetch_monthly_data(
    target_month: int, target_year: int
) -> Sequence[Row[Tuple[DateDimension, int, Any]]]:
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    stmt = (
        select(
            DateDimension,
            func.sum((~DailyAttendance.present).cast(Integer)).label("absence"),
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
        .group_by(DateDimension.date_id)
        .where(DateDimension.mois == target_month)
        .where(DateDimension.annee == target_year)
        .where(DateDimension.jour_semaine.not_in([6, 7]))
        .order_by(DateDimension.date_id)
    )

    with Session(engine) as session:
        return session.execute(stmt).all()


# *--------------------------------- CSV / EXCEL generation -------------------------------------*


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
        f"data/daily_reports/{filename}.csv", mode="w", newline="", encoding="utf-8-sig"
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

    return f"data/daily_reports/{filename}.csv"


@task
def generate_weekly_csv(
    weekly_data: Sequence[Row[Tuple[date, str, bool, int, int, Any]]], filename: str
):
    os.makedirs("data/weekly_reports/", exist_ok=True)
    with open(
        f"data/weekly_reports/{filename}.csv",
        mode="w",
        newline="",
        encoding="utf-8-sig",
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

    return f"data/weekly_reports/{filename}.csv"


@task
def generate_weekly_excel(start_date: date, end_date: date, filename: str) -> str:
    logger = get_run_logger()

    os.makedirs("data/weekly_reports/", exist_ok=True)
    workbook = xlsxwriter.Workbook(f"data/weekly_reports/{filename}.xlsx")
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})

    engine = get_engine()
    logger.info("Fetching data from database")
    with Session(engine) as session:
        services = session.execute(select(DimService.id, DimService.name)).all()
        for service in services:
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
                .join(DimService, DimService.id == DimEmployee.service_id)
                .where(DateDimension.date_literale.between(start_date, end_date))
                .where(DimService.id == service.id)
                .group_by(
                    DateDimension.date_literale,
                    DateDimension.nom_jour,
                    DateDimension.est_ferie,
                )
                .order_by(DateDimension.date_literale.asc())
            )

            table = session.execute(stmt).all()

            cleaned_name = "".join(
                [c for c in service.name if c not in ["'", "/", '"', "@"]]
            )

            worksheet_name = cleaned_name[:31]
            worksheet = workbook.add_worksheet(name=worksheet_name)
            worksheet.set_column(0, 0, 12)  # Date column
            worksheet.set_column(1, 1, 10)  # Day name
            worksheet.set_column(2, 2, 8)  # Férié
            worksheet.set_column(3, 3, 14)  # Absences
            worksheet.set_column(4, 4, 25)  # Total employees
            worksheet.set_column(5, 5, 15)  # Other columns

            headers = [
                "Date",
                "Jour",
                "Férié",
                "Nombre d'absences",
                "Nombre total d'employés",
                "Absence %",
            ]

            # Add a title in the first row
            title = f"Rapport hebdomadaire - {service.name}"
            title_format = workbook.add_format(
                {"align": "center", "bold": True, "font_size": 14}
            )
            worksheet.merge_range(0, 0, 0, len(headers) - 1, title, title_format)

            # Write headers in the second row
            for col, header in enumerate(headers):
                worksheet.write(1, col, header)

            # Write data starting from the third row
            for row_idx, row in enumerate(table, start=2):
                for col_idx, value in enumerate(row):
                    if col_idx == 0 and isinstance(value, date):  # Date column
                        worksheet.write_datetime(row_idx, col_idx, value, date_format)
                    elif col_idx == 2:  # Férié column
                        worksheet.write(row_idx, col_idx, "OUI" if value else "NON")
                    else:
                        worksheet.write(row_idx, col_idx, value)

    workbook.close()
    return f"data/weekly_reports/{filename}.xlsx"


@task
def generate_monthly_excel(target_month: int, target_year: int, filename: str) -> str:
    logger = get_run_logger()

    os.makedirs("data/monthly_reports/", exist_ok=True)
    workbook = xlsxwriter.Workbook(f"data/monthly_reports/{filename}.xlsx")
    date_format = workbook.add_format({"num_format": "yyyy-mm-dd"})

    engine = get_engine()
    logger.info("Fetching data from database")

    with Session(engine) as session:
        services = session.execute(select(DimService.id, DimService.name)).all()
        for service in services:
            stmt = (
                select(
                    DateDimension,
                    func.sum((~DailyAttendance.present).cast(Integer)).label("absence"),
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
                .join(DimService, DimService.id == DimEmployee.service_id)
                .group_by(DateDimension.date_id)
                .where(DateDimension.mois == target_month)
                .where(DateDimension.annee == target_year)
                .where(DateDimension.jour_semaine.not_in([6, 7]))
                .where(DimService.id == service.id)
                .order_by(DateDimension.date_id)
            )

            table = session.execute(stmt).all()

            cleaned_name = "".join(
                [c for c in service.name if c not in ["'", "/", '"', "@"]]
            )

            worksheet_name = cleaned_name[:31]
            worksheet = workbook.add_worksheet(name=worksheet_name)
            worksheet.set_column(0, 0, 12)  # Date column
            worksheet.set_column(1, 1, 10)  # Day name
            worksheet.set_column(2, 2, 14)  # Absences
            worksheet.set_column(3, 3, 14)  # Other columns

            headers = [
                "Date",
                "Jour",
                "Nombre d'absences",
                "Absence %",
            ]

            # Add a title in the first row
            title = f"Rapport mensuel - {service.name}"
            title_format = workbook.add_format(
                {"align": "center", "bold": True, "font_size": 14}
            )
            worksheet.merge_range(0, 0, 0, len(headers) - 1, title, title_format)

            # Write headers in the second row
            for col, header in enumerate(headers):
                worksheet.write(1, col, header)

            # Write data starting from the third row
            for row_idx, row in enumerate(table, start=2):
                date_dim = row[0]
                absence = row[1]
                absence_percentage = row[2]

                # Write date and day name
                worksheet.write_datetime(
                    row_idx, 0, date_dim.date_literale, date_format
                )
                worksheet.write(row_idx, 1, date_dim.nom_jour)
                # Write absence and absence percentage
                worksheet.write(row_idx, 2, absence)
                worksheet.write(row_idx, 3, absence_percentage)

    workbook.close()
    return f"data/monthly_reports/{filename}.xlsx"


if __name__ == "__main__":
    with disable_run_logger():
        service_tables = generate_weekly_excel.fn(
            date(2025, 7, 7), date(2025, 7, 11), "test"
        )
