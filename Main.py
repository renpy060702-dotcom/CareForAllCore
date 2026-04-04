import sqlite3
import smtplib
from email.message import EmailMessage

DB_NAME = "vaccine_system.db"

# Sender email credentials
SENDER_EMAIL = "rentestpy@gmail.com"
SENDER_PASSWORD = "wifk rurp qlix itzd"


# Vaccine schedule flow based on DOH vaccination chart
VACCINE_FLOW = {
    "At Birth": {
        "next_schedule": "1st Visit (1 1/2 Months)",
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Pneumococcal Conjugate Vaccine (PCV)"
        ]
    },
    "First Visit (1 ½ Months)": {
        "next_schedule": "2nd Visit (2 1/2 Months)",
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Pneumococcal Conjugate Vaccine (PCV)"
        ]
    },
    "Second Visit (2 ½ Months)": {
        "next_schedule": "3rd Visit (3 1/2 Months)",
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Inactivated Polio Vaccine (IPV)",
            "Pneumococcal Conjugate Vaccine (PCV)"
        ]
    },
    "Third Visit (3 ½ Months)": {
        "next_schedule": "4th Visit (9 Months)",
        "next_vaccines": [
            "Measles, Mumps, Rubella Vaccine (MMR)"
        ]
    },
    "Fourth Visit (9 Months)": {
        "next_schedule": "5th Visit (1 Year)",
        "next_vaccines": [
            "Measles, Mumps, Rubella Vaccine (MMR)"
        ]
    },
    "Fifth Visit (1 Year)": {
        "next_schedule": None,
        "next_vaccines": []
    }
}


def normalize_schedule(schedule_text):
    """Convert database schedule text into a consistent key."""
    if not schedule_text:
        return None

    schedule_text = schedule_text.strip().lower()

    if "at birth" in schedule_text:
        return "At Birth"
    elif "first visit" in schedule_text or ("1" in schedule_text and "month" in schedule_text):
        return "First Visit (1 ½ Months)"
    elif "second visit" in schedule_text or ("2" in schedule_text and "month" in schedule_text):
        return "Second Visit (2 ½ Months)"
    elif "third visit" in schedule_text or ("3" in schedule_text and "month" in schedule_text):
        return "Third Visit (3 ½ Months)"
    elif "fourth visit" in schedule_text or "9 month" in schedule_text:
        return "Fourth Visit (9 Months)"
    elif "fifth visit" in schedule_text or "1 year" in schedule_text:
        return "Fifth Visit (1 Year)"

    return None


def build_email_body(name, guardian_name, current_schedule, next_schedule, next_vaccines):
    vaccines_text = "\n".join([f"- {v}" for v in next_vaccines])

    return f"""
Dear {guardian_name if guardian_name else 'Parent/Guardian'},

Good day!

This email will start notifying you about your baby's upcoming vaccination schedules to help ensure that {name} receives all recommended vaccines on time.

This is a friendly reminder regarding the next vaccination schedule for {name}.

Current completed schedule:
{current_schedule}

Next vaccination schedule:
{next_schedule}

Recommended vaccine(s) for the next visit:
{vaccines_text}

Please visit your nearest health center on the scheduled date for your baby's next vaccination.

If you have already completed this vaccination, please disregard this message.

Thank you and keep safe.

Care4Core Vaccination Reminder System
""".strip()


def send_email(to_email, subject, body):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = to_email
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
        smtp.send_message(msg)


def main():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, guardian_name, email, vaccine_schedule
        FROM patients
    """)

    rows = cursor.fetchall()
    conn.close()

    print("Checking patients for next vaccine reminders...")
    print("-" * 60)

    for row in rows:
        name, guardian_name, email, vaccine_schedule = row

        print(f"Patient: {name}")
        print(f"Current Schedule in DB: {vaccine_schedule}")

        if not email:
            print("  Skipped: No email address found.")
            print("-" * 60)
            continue

        normalized = normalize_schedule(vaccine_schedule)

        if not normalized:
            print("  Skipped: Schedule not recognized.")
            print("-" * 60)
            continue

        next_info = VACCINE_FLOW.get(normalized)

        if not next_info or not next_info["next_schedule"]:
            print("  No next vaccination schedule. Already completed.")
            print("-" * 60)
            continue

        next_schedule = next_info["next_schedule"]
        next_vaccines = next_info["next_vaccines"]

        subject = f"Vaccination Reminder for {name}"

        body = build_email_body(
            name=name,
            guardian_name=guardian_name,
            current_schedule=normalized,
            next_schedule=next_schedule,
            next_vaccines=next_vaccines
        )

        try:
            send_email(email, subject, body)
            print(f"  Email sent to: {email}")
        except Exception as e:
            print(f"  Failed to send email to {email}: {e}")

        print("-" * 60)


if __name__ == "__main__":
    main()