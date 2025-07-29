import os
from datetime import timedelta
from database.models import DailyReportData, EmailData
from email_service.email_sender import send_daily_email
from email_service.email_generator import (
    generate_report_html,
    generate_csv_from_employees,
)
from prefect import flow
from prefect.logging import get_run_logger
from tasks.queries import (
    fetch_employees_per_date,
    fetch_absent_employees,
    fetch_employees_under_working,
    total_employee_count,
)
from sqlalchemy.orm import Session
from sqlalchemy import select
from database.db import get_engine
from database.models import DateDimension


@flow(flow_run_name="daily-report-{target_date_id}")
def daily_report(target_date_id: int):
    logger = get_run_logger()

    engine = get_engine()

    # Check for holiday
    with Session(engine) as session:
        is_holiday = session.execute(
            select(DateDimension.est_ferie).where(
                DateDimension.date_id == target_date_id
            )
        ).scalar_one()

    if is_holiday:
        logger.info("Today is a holiday. No email will be sent")
        exit(0)

    employees_daily_data = fetch_employees_per_date(target_date_id)

    employee_count = total_employee_count()

    if employees_daily_data is None:
        logger.error("Couldn't fetch employee data")
        exit(1)

    csv_filepath = generate_csv_from_employees(
        employees_daily_data, f"daily_report{target_date_id}.csv"
    )
    employees_under_8_30h = fetch_employees_under_working(employees_daily_data, 8.5)
    employees_under_8h = fetch_employees_under_working(employees_daily_data, 8)
    employees_absent = fetch_absent_employees(employees_daily_data)

    absence_percentage = (len(employees_absent) / employee_count) * 100

    # To get work duration in a more readable string format
    def format_timedelta(td: timedelta) -> str:
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours} heures, {minutes} minutes"

    daily_data = DailyReportData(
        date=target_date_id,
        employees_absent=employees_absent,
        employees_under_8_30h=employees_under_8_30h,
        employees_under_8h=employees_under_8h,
        absence_percentage=absence_percentage,
    )

    html_report = generate_report_html(daily_data)

    email_data = EmailData(
        receiver_emails=["kiretori2003@gmail.com"],
        subject="Rapport Quotidien",
        html_content=html_report,
        csv_file_path=csv_filepath,
    )

    send_daily_email(email_data)
    os.remove(csv_filepath)

    # TODO: finish making of reports


def serve_daily_report_flow():
    daily_report.serve(name="daily-report", cron="0 20 * * *")
