from prefect import flow
from prefect.logging import get_run_logger


@flow
def weekly_report():
    logger = get_run_logger()
    logger.info("Weekly report workflow TO BE IMPLEMENTED")
