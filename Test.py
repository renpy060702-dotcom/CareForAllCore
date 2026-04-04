import sqlite3
import urllib.request
import csv
import io
import time
import smtplib
import calendar
from datetime import datetime, timedelta
from email.message import EmailMessage

# =========================
# CONFIGURATION
# =========================
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQTrsSdJ1LMrRPSJ4MSECO7AN3HJVryQUk-HaYnAXFfoBsxUaaz6us3AXSfJ5Jd05STsJDw5Dp9RJcp/pub?output=csv"
DB_NAME = "vaccine_system.db"
CHECK_INTERVAL = 30  # seconds

SENDER_EMAIL = "rentestpy@gmail.com"
SENDER_PASSWORD = "wifk rurp qlix itzd"

# =========================
# VACCINE FLOW
# =========================
VACCINE_FLOW = {
    "At Birth": {
        "next_schedule": "First Visit (1 ½ Months)",
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Pneumococcal Conjugate Vaccine (PCV)"
        ]
    },
    "First Visit (1 ½ Months)": {
        "next_schedule": "Second Visit (2 ½ Months)",
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Pneumococcal Conjugate Vaccine (PCV)"
        ]
    },
    "Second Visit (2 ½ Months)": {
        "next_schedule": "Third Visit (3 ½ Months)",
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Inactivated Polio Vaccine (IPV)",
            "Pneumococcal Conjugate Vaccine (PCV)"
        ]
    },
    "Third Visit (3 ½ Months)": {
        "next_schedule": "Fourth Visit (9 Months)",
        "next_vaccines": [
            "Measles, Mumps, Rubella Vaccine (MMR)"
        ]
    },
    "Fourth Visit (9 Months)": {
        "next_schedule": "Fifth Visit (1 Year)",
        "next_vaccines": [
            "Measles, Mumps, Rubella Vaccine (MMR)"
        ]
    },
    "Fifth Visit (1 Year)": {
        "next_schedule": None,
        "next_vaccines": []
    }
}

# =========================
# DATABASE FUNCTIONS
# =========================
def connect_db():
    return sqlite3.connect(DB_NAME)


def create_table():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS patients (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT UNIQUE,
        name TEXT,
        age INTEGER,
        guardian_name TEXT,
        contact_number TEXT,
        vaccine_type TEXT,
        vaccine_schedule TEXT,
        date_of_visit TEXT,
        email TEXT,
        last_emailed_schedule TEXT,
        initial_email_sent INTEGER DEFAULT 0
    )
    """)

    conn.commit()
    conn.close()


def ensure_columns_exist():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(patients)")
    columns = [column[1] for column in cursor.fetchall()]

    if "last_emailed_schedule" not in columns:
        cursor.execute("ALTER TABLE patients ADD COLUMN last_emailed_schedule TEXT")

    if "initial_email_sent" not in columns:
        cursor.execute("ALTER TABLE patients ADD COLUMN initial_email_sent INTEGER DEFAULT 0")

    conn.commit()
    conn.close()


def get_patient_by_timestamp(cursor, timestamp):
    cursor.execute("""
        SELECT id, vaccine_schedule, last_emailed_schedule, initial_email_sent
        FROM patients
        WHERE timestamp = ?
    """, (timestamp,))
    return cursor.fetchone()


def insert_patient(cursor, patient_data):
    cursor.execute("""
        INSERT INTO patients (
            timestamp,
            name,
            age,
            guardian_name,
            contact_number,
            vaccine_type,
            vaccine_schedule,
            date_of_visit,
            email,
            last_emailed_schedule,
            initial_email_sent
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, patient_data)


def update_patient_info(cursor, timestamp, age, guardian_name, contact_number,
                        vaccine_type, vaccine_schedule, date_of_visit, email, name):
    cursor.execute("""
        UPDATE patients
        SET name = ?,
            age = ?,
            guardian_name = ?,
            contact_number = ?,
            vaccine_type = ?,
            vaccine_schedule = ?,
            date_of_visit = ?,
            email = ?
        WHERE timestamp = ?
    """, (
        name,
        age,
        guardian_name,
        contact_number,
        vaccine_type,
        vaccine_schedule,
        date_of_visit,
        email,
        timestamp
    ))


def update_last_emailed_schedule(cursor, timestamp, schedule):
    cursor.execute("""
        UPDATE patients
        SET last_emailed_schedule = ?
        WHERE timestamp = ?
    """, (schedule, timestamp))


def mark_initial_email_sent(cursor, timestamp):
    cursor.execute("""
        UPDATE patients
        SET initial_email_sent = 1
        WHERE timestamp = ?
    """, (timestamp,))


# =========================
# GOOGLE SHEET FUNCTIONS
# =========================
def fetch_csv(url):
    with urllib.request.urlopen(url) as response:
        data = response.read()

    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


# =========================
# DATE FUNCTIONS
# =========================
def parse_date(date_str):
    if not date_str:
        return None

    date_str = date_str.strip()

    possible_formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%d-%m-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]

    for fmt in possible_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    return None


def add_months(date_obj, months):
    month = date_obj.month - 1 + months
    year = date_obj.year + month // 12
    month = month % 12 + 1
    day = min(date_obj.day, calendar.monthrange(year, month)[1])
    return date_obj.replace(year=year, month=month, day=day)


def calculate_next_date(current_schedule, visit_date_str):
    visit_date = parse_date(visit_date_str)

    if not visit_date or not current_schedule:
        return None

    if current_schedule == "At Birth":
        next_date = visit_date + timedelta(days=45)
    elif current_schedule == "First Visit (1 ½ Months)":
        next_date = add_months(visit_date, 1)
    elif current_schedule == "Second Visit (2 ½ Months)":
        next_date = add_months(visit_date, 1)
    elif current_schedule == "Third Visit (3 ½ Months)":
        next_date = add_months(visit_date, 5) + timedelta(days=15)
    elif current_schedule == "Fourth Visit (9 Months)":
        next_date = add_months(visit_date, 3)
    else:
        return None

    return next_date.strftime("%B %d, %Y")


# =========================
# SCHEDULE NORMALIZATION
# =========================
def normalize_schedule(schedule_text):
    if not schedule_text:
        return None

    text = schedule_text.strip().lower()

    if "at birth" in text:
        return "At Birth"
    elif "first visit" in text or ("1" in text and "month" in text):
        return "First Visit (1 ½ Months)"
    elif "second visit" in text or ("2" in text and "month" in text):
        return "Second Visit (2 ½ Months)"
    elif "third visit" in text or ("3" in text and "month" in text):
        return "Third Visit (3 ½ Months)"
    elif "fourth visit" in text or "9 month" in text:
        return "Fourth Visit (9 Months)"
    elif "fifth visit" in text or "1 year" in text:
        return "Fifth Visit (1 Year)"

    return None


# =========================
# EMAIL BUILDERS
# =========================
def build_initial_email(name, guardian_name, current_schedule, visit_date):
    return f"""
Dear {guardian_name if guardian_name else "Parent/Guardian"},

Good day!

Thank you for registering {name} in the Care4Core Vaccination Reminder System.

We have successfully received the vaccination form details.

Recorded information:
Name: {name}
Current schedule: {current_schedule if current_schedule else "Not recognized"}
Visit date: {visit_date if visit_date else "Not provided"}

You will receive future vaccination reminders from this system based on the recorded schedule.

Please make sure that your baby's information in the form remains updated.

Thank you and keep safe.

Care4Core Vaccination Reminder System
""".strip()


def build_reminder_email(name, guardian_name, current_schedule, next_schedule, next_vaccines, next_date):
    vaccines_text = "\n".join(f"- {v}" for v in next_vaccines)

    return f"""
Dear {guardian_name if guardian_name else "Parent/Guardian"},

Good day!

This is a friendly reminder regarding the next vaccination schedule for {name}.

Current completed schedule:
{current_schedule}

Next vaccination schedule:
{next_schedule}

Expected next vaccination date:
{next_date if next_date else "Please consult your health center for the exact date."}

Recommended vaccine(s) for the next visit:
{vaccines_text}

Please visit your nearest health center on or before the expected schedule for your baby's next vaccination.

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


# =========================
# MAIN PROCESSING
# =========================
def process_patients():
    try:
        rows = fetch_csv(CSV_URL)

        conn = connect_db()
        cursor = conn.cursor()

        for row in rows:
            timestamp = row.get("Timestamp")
            name = row.get("Pangalan (Apelyido, Pangalan, M.I.)")
            age = row.get("Edad")
            guardian = row.get("Pangalan ng Magulang o Tagapangalaga")
            contact = row.get("Contact Number (ex. 9649127322)")
            vaccine = row.get("Klase ng Bakuna")
            schedule_raw = row.get("Iskedyul ng mga Bakuna")
            visit = row.get("Petsa ng Pagbisita")
            email = row.get("Email address")

            if not timestamp:
                continue

            try:
                age = int(age)
            except (TypeError, ValueError):
                age = None

            current_schedule = normalize_schedule(schedule_raw)
            existing_patient = get_patient_by_timestamp(cursor, timestamp)

            if existing_patient is None:
                insert_patient(cursor, (
                    timestamp,
                    name,
                    age,
                    guardian,
                    contact,
                    vaccine,
                    current_schedule,
                    visit,
                    email,
                    None,
                    0
                ))
                print(f"New patient added: {name}")
                last_emailed_schedule = None
                initial_email_sent = 0
            else:
                _, old_schedule, last_emailed_schedule, initial_email_sent = existing_patient

                update_patient_info(
                    cursor,
                    timestamp,
                    age,
                    guardian,
                    contact,
                    vaccine,
                    current_schedule,
                    visit,
                    email,
                    name
                )

            # Send initial email only once
            if email and initial_email_sent == 0:
                initial_subject = f"Care4Core Registration Confirmation for {name}"
                initial_body = build_initial_email(
                    name=name,
                    guardian_name=guardian,
                    current_schedule=current_schedule,
                    visit_date=visit
                )

                try:
                    send_email(email, initial_subject, initial_body)
                    mark_initial_email_sent(cursor, timestamp)
                    print(f"Initial email sent to {name} ({email})")
                except Exception as e:
                    print(f"Failed to send initial email to {name} ({email}): {e}")

            # Skip reminder if no email
            if not email:
                print(f"Skipped reminder for {name}: no email address.")
                continue

            # Skip reminder if schedule not recognized
            if not current_schedule:
                print(f"Skipped reminder for {name}: schedule not recognized.")
                continue

            # Do not resend reminder for same schedule
            if current_schedule == last_emailed_schedule:
                print(f"No reminder for {name}: already emailed for schedule '{current_schedule}'.")
                continue

            next_info = VACCINE_FLOW.get(current_schedule)

            if not next_info or not next_info["next_schedule"]:
                print(f"No reminder for {name}: no next vaccine schedule available.")
                update_last_emailed_schedule(cursor, timestamp, current_schedule)
                continue

            next_date = calculate_next_date(current_schedule, visit)

            reminder_subject = f"Vaccination Reminder for {name}"
            reminder_body = build_reminder_email(
                name=name,
                guardian_name=guardian,
                current_schedule=current_schedule,
                next_schedule=next_info["next_schedule"],
                next_vaccines=next_info["next_vaccines"],
                next_date=next_date
            )

            try:
                send_email(email, reminder_subject, reminder_body)
                update_last_emailed_schedule(cursor, timestamp, current_schedule)
                print(f"Reminder email sent to {name} ({email}) for schedule '{current_schedule}'")
            except Exception as e:
                print(f"Failed to send reminder email to {name} ({email}): {e}")

        conn.commit()
        conn.close()

    except Exception as e:
        print("Error while processing patients:", e)


# =========================
# MAIN LOOP
# =========================
def main():
    create_table()
    ensure_columns_exist()

    print("Monitoring Google Sheet every 30 seconds...")
    print("-" * 60)

    while True:
        process_patients()
        print("-" * 60)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()