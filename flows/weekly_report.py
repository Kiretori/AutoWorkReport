from prefect import flow
from prefect.logging import get_run_logger
from datetime import date
from tasks.utils import fetch_weekly_data, generate_weekly_csv
from email_service.email_generator import generate_weekly_report_html
from email_service.email_sender import send_daily_email
from database.models import EmailData


@flow
def weekly_report():
    logger = get_run_logger()
    logger.info("Weekly report workflow TO BE IMPLEMENTED")

    start_date, end_date = date(2025, 7, 7), date(2025, 7, 11)
    weekly_data = fetch_weekly_data(start_date, end_date)
    csv_filepath = generate_weekly_csv(
        weekly_data, f"weekly_{start_date}_{end_date}.csv"
    )
    html_report = generate_weekly_report_html(weekly_data, start_date, end_date)

    email_data = EmailData(
        receiver_emails=["kiretori2003@gmail.com"],
        subject="Rapport Hebdomadaire",
        html_content=html_report,
        csv_file_path=csv_filepath,
    )

    send_daily_email(email_data)


def serve_weekly_report_flow():
    weekly_report.serve(name="weekly-report", cron="0 20 * * 5")
