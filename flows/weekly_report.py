from typing import Tuple
from prefect import flow
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.logging import get_run_logger
from datetime import date, timedelta
from tasks.utils import fetch_weekly_data, generate_weekly_excel
from email_service.email_generator import generate_weekly_report_html
from email_service.email_sender import send_daily_email
from database.models import EmailData
from flows import RECEIVER_EMAILS


def get_last_workweek() -> Tuple[date, date]:
    today = date.today()
    last_monday = today - timedelta(days=today.weekday() + 7)
    last_friday = last_monday + timedelta(days=4)
    return last_monday, last_friday


def generate_weekly_flow_name() -> str:
    start, end = get_last_workweek()
    start = start.strftime("%Y%m%d")
    end = end.strftime("%Y%m%d")
    return f"weekly-report-{start}-{end}"


@flow(
    flow_run_name=generate_weekly_flow_name,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def weekly_report():
    logger = get_run_logger()

    start_date, end_date = get_last_workweek()
    weekly_data = fetch_weekly_data.submit(start_date, end_date)
    logger.info("Generating Excel sheets")
    xlsx_filepath = generate_weekly_excel.submit(
        start_date,
        end_date,
        f"weekly_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}",
    )
    html_report = generate_weekly_report_html(
        weekly_data.result(), start_date, end_date
    )

    email_data = EmailData(
        receiver_emails=RECEIVER_EMAILS,
        subject="Rapport Hebdomadaire",
        html_content=html_report,
        report_file_path=f"{xlsx_filepath.result()}",
    )

    send_daily_email(email_data)
