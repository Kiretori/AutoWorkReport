from datetime import date
from typing import Tuple
from database.models import EmailData
from tasks.utils import fetch_quarterly_data, generate_quarterly_excel
from email_service.email_generator import generate_quarterly_report_html
from email_service.email_sender import send_daily_email
from prefect import flow
from prefect.logging import get_run_logger
from flows import RECEIVER_EMAILS


def get_target_quarter_year() -> Tuple[int, int]:
    today = date.today()
    target_quarter = ((today.month - 1) // 3) + 1
    target_year = today.year
    return target_quarter, target_year


def generate_quarterly_flow_name() -> str:
    quarter, year = get_target_quarter_year()
    return f"quaterly-month-Q{quarter}-{year}"


@flow(flow_run_name=generate_quarterly_flow_name)
def quarterly_report(target_quarter: int | None, target_year: int | None):
    logger = get_run_logger()

    _target_quarter, _target_year = get_target_quarter_year()
    if target_quarter is None:
        target_quarter = _target_quarter
    if target_year is None:
        target_year = _target_year

    quarterly_data = fetch_quarterly_data(target_quarter, target_year)
    logger.info("Generating Excel sheets")
    xlsx_filepath = generate_quarterly_excel(
        target_quarter,
        target_year,
        f"quarterly_report_Q{target_quarter}_{target_year}",
    )

    html_report = generate_quarterly_report_html(
        quarterly_data, target_quarter, target_year
    )

    email_data = EmailData(
        receiver_emails=RECEIVER_EMAILS,
        subject="Rapport Trimestriel",
        html_content=html_report,
        report_file_path=f"{xlsx_filepath}",
    )

    send_daily_email(email_data)
