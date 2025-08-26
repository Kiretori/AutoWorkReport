from database.models import DailyReportData, EmailData, DateDimension
from database.db import get_engine
from email_service.email_sender import send_daily_email
from email_service.email_generator import generate_daily_report_html
from prefect import flow
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
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
from flows import RECEIVER_EMAILS


def generate_daily_flow_name() -> str:
    date_id = int(date.today().strftime("%Y%m%d"))
    return f"daily_report-{date_id}"


@flow(
    flow_run_name=generate_daily_flow_name,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def daily_report(target_date_id: int | None = None):
    logger = get_run_logger()

    engine = get_engine()

    if target_date_id is None:
        today = date.today()
        if date.today().weekday() in {5, 6}:  # Skip weekends
            logger.info("Nothing to report on a weekend.")
            exit(0)
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

    employee_count = total_employee_count.submit()

    if employees_daily_data is None:
        logger.error("Couldn't fetch employee data")
        exit(1)

    employees_under_8_30h = fetch_employees_under_working.submit(
        employees_daily_data, 8.5
    )
    employees_under_8h = fetch_employees_under_working.submit(employees_daily_data, 8)
    employees_absent = fetch_absent_employees.submit(employees_daily_data)

    csv_filepath = generate_daily_csv.submit(
        employees_daily_data, f"daily_report{target_date_id}"
    )

    absence_percentage = (
        len(employees_absent.result()) / employee_count.result()
    ) * 100

    daily_data = DailyReportData(
        date=target_date_id,
        employees_absent=employees_absent.result(),
        employees_under_8_30h=employees_under_8_30h.result(),
        employees_under_8h=employees_under_8h.result(),
        absence_percentage=absence_percentage,
    )

    html_report = generate_daily_report_html(daily_data)

    email_data = EmailData(
        receiver_emails=RECEIVER_EMAILS,
        subject="Rapport Quotidien",
        html_content=html_report,
        report_file_path=f"{csv_filepath.result()}",
    )

    send_daily_email(email_data)
