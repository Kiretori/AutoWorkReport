from typing import Dict, List, Sequence
import os
from dotenv import load_dotenv
from datetime import timedelta
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import Session
from database.models import DimEmployee, DailyAttendance, DailyReportData, EmailData
from email_service.email_sender import send_daily_email
from prefect import task, flow

@flow
def extract_data(target_date_id: int):
    load_dotenv()
    DB_URL = os.getenv("DATABASE_URL_LOCAL")
    if DB_URL:
        engine = create_engine(DB_URL)
    else:
        print("Database URL is empty.")
        return

    with Session(engine) as session:
        employee_count = total_employee_count(session)
        employees_under_8_30h = fetch_employees_under_working(
            session, target_date_id, 8.5
        )
        employees_under_8h = fetch_employees_under_working(session, target_date_id, 8)
        employees_absent = fetch_absent_employees(session, target_date_id)

    absence_percentage = (len(employees_absent) / employee_count) * 100

    def format_timedelta(td: timedelta) -> str:
        total_seconds = int(td.total_seconds())
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        return f"{hours} heures, {minutes} minutes"

    daily_data = DailyReportData(
        date=target_date_id,
        employees_absent=[
            f"{employee.last_name} {employee.first_name}" for employee in employees_absent
        ],
        employees_under_8_30h=[
            f"{employee_data['employee'].last_name} {employee_data['employee'].first_name}: {format_timedelta(employee_data['work_hours'])}"  # type: ignore
            for employee_data in employees_under_8_30h
        ],
        employees_under_8h=[
            f"{employee_data['employee'].last_name} {employee_data['employee'].first_name}: {format_timedelta(employee_data['work_hours'])}"  # type: ignore
            for employee_data in employees_under_8h
        ],
        absence_percentage=absence_percentage,
    )

    email_data = EmailData(
        receiver_emails=["kiretori2003@gmail.com"],
        subject="Rapport Quotidien",
        html_content=generate_report_html(daily_data),
    )

    send_daily_email(email_data)

    # TODO: finish making of reports

@task
def generate_report_html(daily_data: DailyReportData) -> str:
    date_str = str(daily_data.date)
    formatted_date = f"{date_str[6:]}/{date_str[4:6]}/{date_str[:4]}"

    # Prepare the absent employees string
    absent_employees_str = (
        ", ".join(daily_data.employees_absent)
        if daily_data.employees_absent
        else "aucun"
    )

    # --- Prepare HTML for employees under 8.5 hours ---
    under_8_30h_list_html = ""
    if daily_data.employees_under_8_30h:
        under_8_30h_list_items = [
            f"<li>{employee}</li>" for employee in daily_data.employees_under_8_30h
        ]
        under_8_30h_list_html = f"<ul>{''.join(under_8_30h_list_items)}</ul>"
    else:
        under_8_30h_list_html = (
            "<p>Aucun employé n'a travaillé moins de 8.5 heures.</p>"
        )

    # --- Prepare HTML for employees under 8 hours ---
    under_8h_list_html = ""
    if daily_data.employees_under_8h:
        under_8h_list_items = [
            f"<li>{employee}</li>" for employee in daily_data.employees_under_8h
        ]
        under_8h_list_html = f"<ul>{''.join(under_8h_list_items)}</ul>"
    else:
        under_8h_list_html = "<p>Aucun employé n'a travaillé moins de 8 heures.</p>"


    



    html_report = f"""
        <!DOCTYPE html>
        <html>
        <head>
        <meta charset="UTF-8">
        <title>Rapport de présence quotidienne</title>
        <style>
            body {{
            font-family: Arial, sans-serif;
            background-color: #f4f4f4;
            padding: 20px;
            }}
            .container {{
            max-width: 600px;
            background-color: #ffffff;
            padding: 20px;
            border-radius: 8px;
            margin: auto;
            box-shadow: 0 0 10px rgba(0,0,0,0.05);
            }}
            h2 {{
            color: #333333;
            text-align: center;
            }}
            table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            }}
            th, td {{
            text-align: left;
            padding: 10px;
            border-bottom: 1px solid #dddddd;
            }}
            th {{
            background-color: #f0f0f0;
            }}
            .footer {{
            font-size: 12px;
            color: #888;
            text-align: center;
            margin-top: 30px;
            }}
            details {{
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 10px;
                margin-top: 20px;
            }}
            summary {{
                font-weight: bold;
                cursor: pointer;
                outline: none;
            }}
            details[open] summary {{
                border-bottom: 1px solid #eee;
                padding-bottom: 10px;
                margin-bottom: 10px;
            }}
            details ul {{
                list-style-type: disc;
                padding-left: 20px;
                margin-top: 0;
            }}
            details p {{
                margin-top: 0;
            }}
        </style>
        </head>
        <body>
        <div class="container">
            <h2>Rapport de présence quotidienne – {formatted_date}</h2>
            <table>
            <tr><th>Nombre d'absence</th><td>{len(daily_data.employees_absent)}</td></tr>
            <tr><th>Taux d'absence</th><td>{daily_data.absence_percentage:.2f}%</td></tr>
            <tr><th>Liste d'absence</th><td>{absent_employees_str}</td></tr>
            <tr><th>Nombre d'employés ayant travaillé moins de 8.5 heures</th><td>{len(daily_data.employees_under_8_30h)}</td></tr>
            <tr><th>Nombre d'employés ayant travaillé moins de 8 heures</th><td>{len(daily_data.employees_under_8h)}</td></tr>
            </table>

            <details>
                <summary>Employés ayant travaillé moins de 8.5 heures</summary>
                {under_8_30h_list_html}
            </details>

            <details>
                <summary>Employés ayant travaillé moins de 8 heures</summary>
                {under_8h_list_html}
            </details>

            <div class="footer">
            Generated automatically by WorkReport System
            </div>
        </div>
        </body>
        </html>
        """

    return html_report


def fetch_employees_under_working(
    session: Session, target_date_id: int, under_work_threshhold: float
) -> List[Dict[str, object]]:
    under_work_hours_timedelta = timedelta(hours=under_work_threshhold)
    work_duration = DailyAttendance.check_out_hour - DailyAttendance.check_in_hour

    stmt = (
        select(DimEmployee, work_duration)
        .join(DailyAttendance, DimEmployee.id == DailyAttendance.id_employee)
        .where(DailyAttendance.date_id == target_date_id)
        .where(DailyAttendance.present)
        .where(
            (DailyAttendance.check_out_hour - DailyAttendance.check_in_hour)
            < under_work_hours_timedelta
        )
    )

    employees_under_working = session.execute(stmt).all()
    return [
        {"employee": row[0], "work_hours": row[1]} for row in employees_under_working
    ]


def fetch_absent_employees(session: Session, target_date_id: int) -> Sequence[DimEmployee]:
    stmt = (
        select(DimEmployee)
        .join(DailyAttendance, DimEmployee.id == DailyAttendance.id_employee)
        .where(DailyAttendance.date_id == target_date_id)
        .where(~DailyAttendance.present)
    )

    employees_absent = session.execute(stmt).scalars().all()

    return employees_absent


def total_employee_count(session: Session) -> int:
    stmt = select(func.count()).select_from(DimEmployee)
    employee_count = session.execute(stmt).scalar_one()
    return employee_count


def start_pipeline():
    extract_data.serve(
        name="daily-report",
        cron="0 20 * * *"
    )

if __name__ == "__main__":
    start_pipeline()