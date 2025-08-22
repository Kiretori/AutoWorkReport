from tasks.warehousing import (
    refresh_monthly_employee_absence_mv,
    refresh_monthly_service_absence_mv,
)
from prefect import flow
from prefect.logging import get_run_logger


@flow()
def refresh_warehouse():
    logger = get_run_logger()

    logger.info("Starting data warehouse refresh...")

    refresh_monthly_employee_absence_mv()
    refresh_monthly_service_absence_mv()

    logger.info("Data warehouse REFRESHED")
