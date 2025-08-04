from database.models import DailyReportData, EmailData, DateDimension
from database.db import get_engine
from email_service.email_sender import send_daily_email
from email_service.email_generator import generate_daily_report_html
from prefect import flow
from prefect.logging import get_run_logger
from tasks.utils import (
    fetch_employees_per_date,
    fetch_absent_employees,
    fetch_employees_under_working,
    total_employee_count,
    generate_daily_csv,
)
from sqlalchemy.orm import Session
from sqlalchemy import select
from datetime import date


def generate_daily_flow_name() -> str:
    date_id = int(date.today().strftime("%Y%m%d"))
    return f"daily_report-{date_id}"


@flow(flow_run_name=generate_daily_flow_name)
def daily_report(target_date_id: int | None = None):
    logger = get_run_logger()

    engine = get_engine()

    if target_date_id is None:
        today = date.today()
        with Session(engine) as session:
            target_date_id = session.execute(
                select(DateDimension.date_id).where(
                    DateDimension.date_literale == today
                )
            ).scalar_one_or_none()

        if target_date_id is None:
            logger.error(f"No date_id found for today's date: {today}")
            return

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

    csv_filepath = generate_daily_csv(
        employees_daily_data, f"daily_report{target_date_id}"
    )
    employees_under_8_30h = fetch_employees_under_working(employees_daily_data, 8.5)
    employees_under_8h = fetch_employees_under_working(employees_daily_data, 8)
    employees_absent = fetch_absent_employees(employees_daily_data)

    absence_percentage = (len(employees_absent) / employee_count) * 100

    daily_data = DailyReportData(
        date=target_date_id,
        employees_absent=employees_absent,
        employees_under_8_30h=employees_under_8_30h,
        employees_under_8h=employees_under_8h,
        absence_percentage=absence_percentage,
    )

    html_report = generate_daily_report_html(daily_data)

    email_data = EmailData(
        receiver_emails=["kiretori2003@gmail.com"],
        subject="Rapport Quotidien",
        html_content=html_report,
        report_file_path=f"{csv_filepath}",
    )

    send_daily_email(email_data)


def serve_daily_report_flow():
    daily_report.serve(name="daily-report", cron="0 20 * * *")
