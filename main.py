from prefect import serve
from flows.daily_report import daily_report
from flows.weekly_report import weekly_report

if __name__ == "__main__":
    daily_flow_deploy = daily_report.to_deployment(
        name="daily-report", cron="0 20 * * *"
    )
    weekly_flow_deploy = weekly_report.to_deployment(
        name="weekly-report", cron="0 20 * * 5"
    )

    serve(daily_flow_deploy, weekly_flow_deploy)  # type: ignore
