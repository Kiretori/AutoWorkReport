import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List
from database.models import EmailData
from dotenv import load_dotenv
import os
from prefect import task
from prefect.logging import get_run_logger, disable_run_logger
from database.db import get_engine
from sqlalchemy.orm import aliased, Session
from sqlalchemy import select
from database.models import DimEmployee


@task
def extract_receiver_emails() -> List[str]:
    logger = get_run_logger()
    try:
        engine = get_engine()
    except Exception as e:
        logger.error(f"Error while connecting to the database: {e}")
        exit(1)

    manager = aliased(DimEmployee)
    employee = aliased(DimEmployee)

    with Session(engine) as session:
        return list(
            session.execute(
                select(manager.email)
                .join(employee, employee.hierarchical_manager_id == manager.id)
                .distinct()
            )
            .scalars()
            .all()
        )


@task(retries=3, timeout_seconds=20)
def send_daily_email(email_data: EmailData):
    load_dotenv()
    logger = get_run_logger()

    sender_email = os.getenv("GMAIL_USER")
    app_password = os.getenv("GMAIL_APP_PASS")

    if not sender_email or not app_password:
        logger.error(
            "Error: GMAIL_USER and GMAIL_APP_PASS environment variables are not set."
        )
        logger.error("Please set them before running the script for security reasons.")
        exit()

    if email_data.receiver_emails is None:
        receiver_emails = extract_receiver_emails()
    else:
        receiver_emails = email_data.receiver_emails

    # Email Content
    message = MIMEMultipart("alternative")
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails)
    message["Subject"] = email_data.subject

    # Attach html to the message
    message.attach(MIMEText(email_data.html_content, "html"))
    if email_data.report_file_path:
        try:
            with open(email_data.report_file_path, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            encoders.encode_base64(part)

            filename = os.path.basename(email_data.report_file_path)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={filename}",
            )

            message.attach(part)
            logger.info(f"Attached file: {filename}")
        except Exception as e:
            logger.error(f"Failed to attach file '{email_data.report_file_path}': {e}")

    # Convert message to string
    email_text = message.as_string()

    # SMTP Server Details for Gmail
    smtp_server = "smtp.gmail.com"
    port = 587  # For TLS

    # Create a secure SSL context
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    # Send the Email
    logger.info(f"Attempting to send email from {sender_email} to {receiver_emails}...")
    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls(context=context)  # Use secure TLS connection
            server.login(sender_email, app_password)
            server.sendmail(sender_email, receiver_emails, email_text)
        logger.info("Email sent successfully!")
    except smtplib.SMTPAuthenticationError as e:
        logger.error(
            f"Authentication Error: Check your Gmail address and App Password. Details: {e}"
        )
        logger.error(
            "Make sure 2-Step Verification is enabled and you're using an App Password, not your regular Gmail password."
        )
    except Exception as e:
        logger.error(f"An error occurred while sending the email: {e}")


if __name__ == "__main__":
    with disable_run_logger():
        for email in extract_receiver_emails.fn():
            print(email)
