from prefect import task
from database.models import DailyReportData
import csv
import os 
from typing import Tuple

@task
def generate_report_html(daily_data: DailyReportData) -> Tuple[str, str]:
    date_str = str(daily_data.date)
    formatted_date = f"{date_str[6:]}/{date_str[4:6]}/{date_str[:4]}"

    # Prepare the absent employees string
    absent_employees_str = (
        ", ".join(daily_data.employees_absent)
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
                {'<ul class="scrollable-list">' + "".join(f"<li>{e}</li>" for e in daily_data.employees_under_8_30h) + "</ul>" if daily_data.employees_under_8_30h else "<p>Aucun employé n'a travaillé moins de 8.5 heures.</p>"}
            </details>

            <details>
                <summary>Employés ayant travaillé moins de 8 heures</summary>
                {'<ul class="scrollable-list">' + "".join(f"<li>{e}</li>" for e in daily_data.employees_under_8h) + "</ul>" if daily_data.employees_under_8h else "<p>Aucun employé n'a travaillé moins de 8 heures.</p>"}
            </details>

            <div class="footer">
                Généré automatiquement par le système WorkReport
            </div>
        </div>
    </body>
    </html>
    """
    file_path = generate_csv_from_report(daily_data)
    return html_report, file_path


def generate_csv_from_report(daily_data, filename: str = "daily_report.csv"):
    os.makedirs("data", exist_ok=True)
    with open(f"data/{filename}", mode='w', newline='', encoding='utf-8-sig') as file:
        writer = csv.writer(file)

        # Header
        writer.writerow(["Champ", "Valeur"])

        # Summary rows
        writer.writerow(["Date", str(daily_data.date)])
        writer.writerow(["Nombre d'absence", len(daily_data.employees_absent)])
        writer.writerow(["Taux d'absence", f"{daily_data.absence_percentage:.2f}%"])
        writer.writerow(["Employés absents", ", ".join(daily_data.employees_absent) or "aucun"])
        writer.writerow(["Employés < 8.5h", len(daily_data.employees_under_8_30h)])
        writer.writerow(["Employés < 8h", len(daily_data.employees_under_8h)])

        # Spacer
        writer.writerow([])

        # Detailed lists
        writer.writerow(["Employés ayant travaillé moins de 8.5 heures"])
        writer.writerows([[e] for e in daily_data.employees_under_8_30h or ["aucun"]])

        writer.writerow([])

        writer.writerow(["Employés ayant travaillé moins de 8 heures"])
        writer.writerows([[e] for e in daily_data.employees_under_8h or ["aucun"]])

    return f"data/{filename}"
