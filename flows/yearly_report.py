from prefect import flow
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from datetime import date
from database.models import EmailData
from tasks.utils import fetch_yearly_data, generate_yearly_excel
from email_service.email_generator import generate_yearly_report_html
from email_service.email_sender import send_daily_email
from flows import RECEIVER_EMAILS


def generate_yearly_flow_name() -> str:
    return f"yearly-report-{date.today().year}"


@flow(
    flow_run_name=generate_yearly_flow_name,
    task_runner=ThreadPoolTaskRunner(max_workers=4),
)
def yearly_report(target_year: int | None = None):
    logger = get_run_logger()

    if target_year is None:
        target_year = date.today().year

    yearly_data = fetch_yearly_data.submit(target_year)
    logger.info("Generating Excel sheets")
    xlsx_filepath = generate_yearly_excel.submit(
        target_year,
        f"yearly_report_{target_year}",
    )

    html_report = generate_yearly_report_html(yearly_data.result(), target_year)

    email_data = EmailData(
        receiver_emails=RECEIVER_EMAILS,
        subject="Rapport Annuel",
        html_content=html_report,
        report_file_path=f"{xlsx_filepath.result()}",
    )

    send_daily_email(email_data)
