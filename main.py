from prefect import serve
from prefect.schedules import RRule
from flows.daily_report import daily_report
from flows.weekly_report import weekly_report
from flows.monthly_report import monthly_report
from flows.quarterly_report import quarterly_report
from flows.yearly_report import yearly_report

if __name__ == "__main__":
    monthly_schedule = RRule("FREQ=MONTHLY;BYMONTHDAY=-1;BYHOUR=20;BYMINUTE=0")
    quarterly_schedule = RRule(
        "FREQ=YEARLY;BYMONTH=3,6,9,12;BYMONTHDAY=-1;BYHOUR=20;BYMINUTE=0"
    )
    yearly_schedule = RRule("FREQ=YEARLY;BYMONTH=12;BYMONTHDAY=-1;BYHOUR=20;BYMINUTE=0")

    daily_flow_deploy = daily_report.to_deployment(
        name="daily-report", cron="0 20 * * *"
    )
    weekly_flow_deploy = weekly_report.to_deployment(
        name="weekly-report", cron="0 20 * * 5"
    )
    monthly_flow_deploy = monthly_report.to_deployment(
        name="monthly-report", schedule=monthly_schedule
    )
    quarterly_flow_deploy = quarterly_report.to_deployment(
        name="quarterly-report", schedule=quarterly_schedule
    )
    yearly_flow_deploy = yearly_report.to_deployment(
        name="yearly-report", schedule=yearly_schedule
    )

    serve(
        daily_flow_deploy,  # type: ignore
        weekly_flow_deploy,  # type: ignore
        monthly_flow_deploy,  # type: ignore
        quarterly_flow_deploy,  # type: ignore
        yearly_flow_deploy,  # type: ignore
    )
