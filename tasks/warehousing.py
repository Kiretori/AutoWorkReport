from prefect import task
from prefect.logging import get_run_logger, disable_run_logger
from database.db import get_engine
from sqlalchemy import text


@task
def refresh_monthly_employee_absence_mv():
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    logger.info("Refreshing hr_data.mv_employee_absence_monthly materialized view...")

    with engine.begin() as conn:
        conn.execute(
            text("REFRESH MATERIALIZED VIEW hr_data.mv_employee_absence_monthly")
        )

    logger.info("hr_data.mv_employee_absence_monthly materialized view REFRESHED")


@task
def refresh_monthly_service_absence_mv():
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    logger.info("Refreshing hr_data.mv_service_absence_monthly materialized view...")

    with engine.begin() as conn:
        conn.execute(
            text("REFRESH MATERIALIZED VIEW hr_data.mv_service_absence_monthly")
        )

    logger.info("hr_data.mv_service_absence_monthly materialized view REFRESHED")


if __name__ == "__main__":
    with disable_run_logger():
        refresh_monthly_service_absence_mv.fn()
