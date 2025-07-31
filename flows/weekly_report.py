from prefect import flow
from prefect.logging import get_run_logger
from datetime import date
from tasks.queries import fetch_employee_weekly_data


@flow
def weekly_report():
    logger = get_run_logger()
    logger.info("Weekly report workflow TO BE IMPLEMENTED")

    start_date, end_date = date(2025, 7, 7), date(2025, 7, 11)
    weekly_data = fetch_employee_weekly_data(start_date, end_date)
    print(weekly_data)
