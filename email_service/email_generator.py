from datetime import date
from typing import Any, Sequence, Tuple
from prefect import task
from sqlalchemy import Row
from database.models import DailyReportData, DateDimension


@task
def generate_daily_report_html(daily_data: DailyReportData) -> str:
    date_str = str(daily_data.date)
    formatted_date = (
        f"{date_str[6:]}/{date_str[4:6]}/{date_str[:4]}"  # Format date as DD/MM/YYYY
    )

    # Prepare the absent employees string
    absent_employees_str = (
        ", ".join(
            [f"{e.last_name} {e.first_name}" for e in daily_data.employees_absent]
        )
        if daily_data.employees_absent
        else "aucun"
    )

    html_report = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <title>Rapport de présence quotidienne</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f9fafb;
                padding: 30px;
                margin: 0;
                color: #1f2937;
            }}
            .container {{
                max-width: 700px;
                margin: auto;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
                padding: 30px;
            }}
            h2 {{
                text-align: center;
                color: #111827;
                margin-bottom: 30px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 30px;
            }}
            th, td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e5e7eb;
                font-size: 14px;
                color: #1f2937;
            }}
            th {{
                background-color: #f3f4f6;
                font-weight: 600;
                text-align: left;
            }}
            details {{
                border: 1px solid #e5e7eb;
                border-radius: 8px;
                margin-bottom: 20px;
                background-color: #f9fafb;
                padding: 15px;
            }}
            summary {{
                font-weight: 600;
                font-size: 15px;
                cursor: pointer;
                color: #111827;
            }}
            details[open] summary {{
                margin-bottom: 10px;
                border-bottom: 1px solid #e5e7eb;
                padding-bottom: 10px;
            }}
            .scrollable-list {{
                max-height: 150px;
                overflow-y: auto;
                padding-left: 20px;
                margin: 0;
                color: #1f2937;
            }}
            .scrollable-list li {{
                margin-bottom: 6px;
                font-size: 14px;
            }}
            .footer {{
                text-align: center;
                font-size: 12px;
                color: #6b7280;
                margin-top: 40px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>Rapport de présence quotidienne – {formatted_date}</h2>
            <table>
                <tr>
                    <th>Nombre d'absence</th>
                    <td>{len(daily_data.employees_absent)}</td>
                </tr>
                <tr>
                    <th>Taux d'absence</th>
                    <td>{daily_data.absence_percentage:.2f}%</td>
                </tr>
                <tr>
                    <th>Liste d'absence</th>
                    <td>{absent_employees_str}</td>
                </tr>
                <tr>
                    <th>Moins de 8.5 heures</th>
                    <td>{len(daily_data.employees_under_8_30h)}</td>
                </tr>
                <tr>
                    <th>Moins de 8 heures</th>
                    <td>{len(daily_data.employees_under_8h)}</td>
                </tr>
            </table>

            <details>
                <summary>Employés ayant travaillé moins de 8.5 heures</summary>
                {'<ul class="scrollable-list">' + "".join(f"<li>{e[0].last_name} {e[0].first_name}: {e[1]}</li>" for e in daily_data.employees_under_8_30h) + "</ul>" if daily_data.employees_under_8_30h else "<p>Aucun employé n'a travaillé moins de 8.5 heures.</p>"}
            </details>

            <details>
                <summary>Employés ayant travaillé moins de 8 heures</summary>
                {'<ul class="scrollable-list">' + "".join(f"<li>{e[0].last_name} {e[0].first_name}: {e[1]}</li>" for e in daily_data.employees_under_8h) + "</ul>" if daily_data.employees_under_8h else "<p>Aucun employé n'a travaillé moins de 8 heures.</p>"}
            </details>

            <div class="footer">
                Généré automatiquement par le système WorkReport
            </div>
        </div>
    </body>
    </html>
    """

    return html_report


@task
def generate_weekly_report_html(
    weekly_data: Sequence[Row[Tuple[date, str, bool, int, int, Any]]],
    start_date: date,
    end_date: date,
) -> str:
    table_rows = ""
    for date_literal, day_name, is_holiday, abs_count, _, abs_percentage in weekly_data:
        table_rows += f"""
        <tr>
            <td>{date_literal}</td>
            <td>{day_name}</td>
            <td>{"OUI" if is_holiday else "NON"}</td>
            <td>{abs_count}</td>
            <td>{abs_percentage}%</td>
        </tr>
        """

    html_report = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <title>Rapport de la semaine du {start_date}</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f9fafb;
                padding: 30px;
                margin: 0;
                color: #1f2937;
            }}
            .container {{
                max-width: 800px;
                margin: auto;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
                padding: 30px;
            }}
            h2 {{
                text-align: center;
                color: #111827;
                margin-bottom: 30px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}
            th, td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e5e7eb;
                font-size: 14px;
                text-align: left;
                color: #1f2937;
            }}
            th {{
                background-color: #f3f4f6;
                font-weight: 600;
            }}
            .footer {{
                text-align: center;
                font-size: 12px;
                color: #6b7280;
                margin-top: 40px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>Rapport hebdomadaire des absences</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Jour</th>
                        <th>Férié</th>
                        <th>Nombre d'absences</th>
                        <th>Pourcentage d'absence</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
            <div class="footer">
                Généré automatiquement par le système WorkReport
            </div>
        </div>
    </body>
    </html>
    """
    return html_report


@task
def generate_monthly_report_html(
    monthly_data: Sequence[Row[Tuple[DateDimension, int, Any]]],
    month_name: str,
    year: int,
) -> str:
    table_rows = ""
    for dim_date, abs_count, abs_percentage in monthly_data:
        table_rows += f"""
        <tr>
            <td>{dim_date.date_literale}</td>
            <td>{dim_date.nom_jour}</td>
            <td>{abs_count}</td>
            <td>{abs_percentage}%</td>
        </tr>
        """

    html_report = f"""
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <title>Rapport de la semaine du {month_name} - {year}</title>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f9fafb;
                padding: 30px;
                margin: 0;
                color: #1f2937;
            }}
            .container {{
                max-width: 800px;
                margin: auto;
                background-color: #ffffff;
                border-radius: 12px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.05);
                padding: 30px;
            }}
            h2 {{
                text-align: center;
                color: #111827;
                margin-bottom: 30px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 20px;
            }}
            th, td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e5e7eb;
                font-size: 14px;
                text-align: left;
                color: #1f2937;
            }}
            th {{
                background-color: #f3f4f6;
                font-weight: 600;
            }}
            .footer {{
                text-align: center;
                font-size: 12px;
                color: #6b7280;
                margin-top: 40px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h2>Rapport des absences - {month_name} {year}</h2>
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Jour</th>
                        <th>Nombre d'absences</th>
                        <th>Pourcentage d'absence</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
            <div class="footer">
                Généré automatiquement par le système WorkReport
            </div>
        </div>
    </body>
    </html>
    """
    return html_report
