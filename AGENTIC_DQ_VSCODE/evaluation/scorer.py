# evaluation/scorer.py
from llm.gemini_client import model
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from config.settings import (
    EMAIL_SENDER,
    EMAIL_PASSWORD,
    EMAIL_RECIPIENTS,
    SMTP_SERVER,
    SMTP_PORT,
    EMAILS_CONFIGURED,
)

logger = logging.getLogger(__name__)

def score_rules(rules: list, pii_fields: list, metrics: dict) -> str:
    """Score rules using Gemini."""
    prompt = f"""
    Rules: {rules}
    PII: {pii_fields}
    Metrics: {metrics}
    Rate quality 1-10 and explain.
    """
    response = model.generate_content(prompt)
    return response.text

def send_email_alert(table: str, metrics: dict, score: str):
    """Send DQ alert via email to configured recipients."""
    # Skip if email not configured
    if not EMAILS_CONFIGURED:
        logger.debug(f"Email not configured. Skipping alert for {table}.")
        return
    
    # Ensure EMAIL_RECIPIENTS is a list
    recipients = EMAIL_RECIPIENTS if isinstance(EMAIL_RECIPIENTS, list) else [EMAIL_RECIPIENTS]
    
    if not recipients:
        logger.info(f"No email recipients configured. Alert for {table}: Pass Rate = {metrics['pass_rate']:.2%}")
        return

    subject = f"[DQ Alert] {table} - Pass Rate: {metrics['pass_rate']:.2%}"
    body = f"""
Data Quality Report for table: {table}

- Total Rows: {metrics['total']}
- Passed: {metrics['passed']}
- Failed: {metrics['failed']}
- Pass Rate: {metrics['pass_rate']:.2%}

Rule Quality Score:
{score}

Best regards,
Agentic DQ Pipeline
    """.strip()

    msg = MIMEMultipart()
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        logger.info(f"Email alert sent for {table} to {len(recipients)} recipient(s).")
    except Exception as e:
        logger.error(f"Failed to send email alert for {table}: {e}")