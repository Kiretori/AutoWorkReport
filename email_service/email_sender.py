import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from database.models import EmailData
from dotenv import load_dotenv
import os


def send_daily_email(email_data: EmailData):
    load_dotenv()

    sender_email = os.getenv("GMAIL_USER")
    app_password = os.getenv("GMAIL_APP_PASS")

    if not sender_email or not app_password:
        print("Error: GMAIL_USER and GMAIL_APP_PASS environment variables are not set.")
        print("Please set them before running the script for security reasons.")
        exit()

    # Email Content 
    message = MIMEMultipart("alternative")
    message["From"] = sender_email
    message["To"] = ", ".join(email_data.receiver_emails)
    message["Subject"] = email_data.subject

    # Attach html to the message
    message.attach(MIMEText(email_data.html_content, "html"))

    # Convert message to string
    email_text = message.as_string()

    # SMTP Server Details for Gmail
    smtp_server = "smtp.gmail.com"
    port = 587  # For TLS

    # Create a secure SSL context
    context = ssl.create_default_context()

    # Send the Email 
    print(f"Attempting to send email from {sender_email} to {email_data.receiver_emails}...")
    try:
        with smtplib.SMTP(smtp_server, port) as server:
            server.starttls(
                context=context
            )  # Use secure TLS connection
            server.login(sender_email, app_password)
            server.sendmail(sender_email, email_data.receiver_emails, email_text)
        print("Email sent successfully!")
    except smtplib.SMTPAuthenticationError as e:
        print(
            f"Authentication Error: Check your Gmail address and App Password. Details: {e}"
        )
        print(
            "Make sure 2-Step Verification is enabled and you're using an App Password, not your regular Gmail password."
        )
    except Exception as e:
        print(f"An error occurred while sending the email: {e}")
