from prefect import serve
from prefect.schedules import RRule
from flows.daily_report import daily_report
from flows.weekly_report import weekly_report
from flows.monthly_report import monthly_report

if __name__ == "__main__":
    monthly_schedule = RRule("FREQ=MONTHLY;BYMONTHDAY=-1;BYHOUR=20;BYMINUTE=0")

    daily_flow_deploy = daily_report.to_deployment(
        name="daily-report", cron="0 20 * * *"
    )
    weekly_flow_deploy = weekly_report.to_deployment(
        name="weekly-report", cron="0 20 * * 5"
    )
    monthly_flow_deploy = monthly_report.to_deployment(
        name="monthly-report", schedule=monthly_schedule
    )

    serve(daily_flow_deploy, weekly_flow_deploy, monthly_flow_deploy)  # type: ignore
