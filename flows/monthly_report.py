from typing import Tuple
from prefect import flow
from prefect.logging import get_run_logger
from email_service.email_generator import generate_monthly_report_html
from email_service.email_sender import send_daily_email
from tasks.utils import fetch_monthly_data, generate_monthly_excel
from database.models import EmailData
from datetime import date


months_map = {
    1: "Janvier",
    2: "Février",
    3: "Mars",
    4: "Avril",
    5: "Mai",
    6: "Juin",
    7: "Juillet",
    8: "Août",
    9: "Septembre",
    10: "Octobre",
    11: "Novembre",
    12: "Décembre",
}


def get_target_month_year() -> Tuple[int, int]:
    today = date.today()
    return today.month, today.year


def generate_monthly_flow_name() -> str:
    month, year = get_target_month_year()
    return f"monthly-report-{month}-{year}"


@flow(flow_run_name=generate_monthly_flow_name)
def monthly_report(target_month: int | None = None, target_year: int | None = None):
    logger = get_run_logger()

    _target_month, _target_year = get_target_month_year()
    if target_month is None:
        target_month = _target_month
    if target_year is None:
        target_year = _target_year

    monthly_data = fetch_monthly_data(target_month, target_year)
    logger.info("Generating excel sheets")
    xlsx_filepath = generate_monthly_excel(
        target_month,
        target_year,
        f"monthly_report_{target_month}_{target_year}",
    )

    html_report = generate_monthly_report_html(
        monthly_data, months_map[target_month], target_year
    )

    email_data = EmailData(
        receiver_emails=["kiretori2003@gmail.com"],
        subject="Rapport Mensuel",
        html_content=html_report,
        report_file_path=f"{xlsx_filepath}",
    )

    send_daily_email(email_data)
