import sqlite3
import urllib.request
import csv
import io
import time
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage

# =========================
# CONFIGURATION
# =========================
CSV_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQTrsSdJ1LMrRPSJ4MSECO7AN3HJVryQUk-HaYnAXFfoBsxUaaz6us3AXSfJ5Jd05STsJDw5Dp9RJcp/pub?output=csv"
DB_NAME = "vaccine_system.db"
CHECK_INTERVAL = 5  # seconds

SENDER_EMAIL = "rentestpy@gmail.com"
SENDER_PASSWORD = "wifk rurp qlix itzd"

# Set to True for quick testing, False for real-date based reminders
TEST_MODE = True

# =========================
# TEST DELAYS
# =========================
TEST_DELAY_MAP = {
    "At Birth": 30,
    "First Visit (1 ½ Months)": 60,
    "Second Visit (2 ½ Months)": 90,
    "Third Visit (3 ½ Months)": 120,
    "Fourth Visit (9 Months)": 150,
    "Fifth Visit (1 Year)": 180,
}

# =========================
# VACCINE FLOW
# =========================
VACCINE_FLOW = {
    "At Birth": {
        "next_schedule": "First Visit (1 ½ Months)",
        "days_until_next": 45,
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Pneumococcal Conjugate Vaccine (PCV)",
        ],
    },
    "First Visit (1 ½ Months)": {
        "next_schedule": "Second Visit (2 ½ Months)",
        "days_until_next": 30,
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Pneumococcal Conjugate Vaccine (PCV)",
        ],
    },
    "Second Visit (2 ½ Months)": {
        "next_schedule": "Third Visit (3 ½ Months)",
        "days_until_next": 30,
        "next_vaccines": [
            "Pentavalent Vaccine (DPT-Hep B-HIB)",
            "Oral Polio Vaccine (OPV)",
            "Inactivated Polio Vaccine (IPV)",
            "Pneumococcal Conjugate Vaccine (PCV)",
        ],
    },
    "Third Visit (3 ½ Months)": {
        "next_schedule": "Fourth Visit (9 Months)",
        "days_until_next": 165,
        "next_vaccines": [
            "Measles, Mumps, Rubella Vaccine (MMR)",
        ],
    },
    "Fourth Visit (9 Months)": {
        "next_schedule": "Fifth Visit (1 Year)",
        "days_until_next": 90,
        "next_vaccines": [
            "Measles, Mumps, Rubella Vaccine (MMR)",
        ],
    },
    "Fifth Visit (1 Year)": {
        "next_schedule": None,
        "days_until_next": None,
        "next_vaccines": [],
    },
}


# =========================
# DATABASE
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
            password TEXT,
            consent TEXT,
            admission_type TEXT,
            name TEXT,
            age INTEGER,
            guardian_name TEXT,
            contact_number TEXT,
            email TEXT,
            vaccine_type TEXT,
            vaccine_schedule TEXT,
            date_of_visit TEXT,
            last_emailed_schedule TEXT,
            initial_email_sent INTEGER DEFAULT 0,
            schedule_changed_at TEXT
        )
    """)

    conn.commit()
    conn.close()


def ensure_columns_exist():
    conn = connect_db()
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(patients)")
    existing_columns = [col[1] for col in cursor.fetchall()]

    required_columns = {
        "password": "TEXT",
        "consent": "TEXT",
        "admission_type": "TEXT",
        "last_emailed_schedule": "TEXT",
        "initial_email_sent": "INTEGER DEFAULT 0",
        "schedule_changed_at": "TEXT",
    }

    for column_name, column_type in required_columns.items():
        if column_name not in existing_columns:
            cursor.execute(f"ALTER TABLE patients ADD COLUMN {column_name} {column_type}")

    conn.commit()
    conn.close()


def get_patient_by_timestamp(cursor, timestamp):
    cursor.execute("""
        SELECT id, vaccine_schedule, last_emailed_schedule, initial_email_sent, schedule_changed_at
        FROM patients
        WHERE timestamp = ?
    """, (timestamp,))
    return cursor.fetchone()


def insert_patient(cursor, data):
    cursor.execute("""
        INSERT INTO patients (
            timestamp,
            password,
            consent,
            admission_type,
            name,
            age,
            guardian_name,
            contact_number,
            email,
            vaccine_type,
            vaccine_schedule,
            date_of_visit,
            last_emailed_schedule,
            initial_email_sent,
            schedule_changed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, data)


def update_patient_info(
    cursor,
    timestamp,
    password,
    consent,
    admission_type,
    name,
    age,
    guardian_name,
    contact_number,
    email,
    vaccine_type,
    vaccine_schedule,
    date_of_visit
):
    cursor.execute("""
        UPDATE patients
        SET password = ?,
            consent = ?,
            admission_type = ?,
            name = ?,
            age = ?,
            guardian_name = ?,
            contact_number = ?,
            email = ?,
            vaccine_type = ?,
            vaccine_schedule = ?,
            date_of_visit = ?
        WHERE timestamp = ?
    """, (
        password,
        consent,
        admission_type,
        name,
        age,
        guardian_name,
        contact_number,
        email,
        vaccine_type,
        vaccine_schedule,
        date_of_visit,
        timestamp
    ))


def update_schedule_and_time(cursor, timestamp, new_schedule, changed_at):
    cursor.execute("""
        UPDATE patients
        SET vaccine_schedule = ?, schedule_changed_at = ?
        WHERE timestamp = ?
    """, (new_schedule, changed_at, timestamp))


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
# GOOGLE SHEETS
# =========================
def fetch_csv(url):
    with urllib.request.urlopen(url) as response:
        data = response.read()

    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    return list(reader)


def clean_value(value):
    if value is None:
        return ""
    return str(value).strip()


def get_value(row, *possible_keys):
    for key in possible_keys:
        if key in row:
            return clean_value(row.get(key))
    return ""


# =========================
# DATE / TIME HELPERS
# =========================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_datetime(datetime_str):
    if not datetime_str:
        return None

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
        "%B %d, %Y %H:%M:%S",
        "%B %d, %Y",
        "%b %d, %Y %H:%M:%S",
        "%b %d, %Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(datetime_str.strip(), fmt)
        except ValueError:
            pass

    return None


def parse_date_only(date_str):
    if not date_str:
        return None

    formats = [
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%m/%d/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            pass

    return None


def format_vaccination_date(dt):
    if not dt:
        return "Not available"
    return dt.strftime("%B %d, %Y")


def calculate_next_vaccination_date(current_schedule, visit_date):
    if not current_schedule or not visit_date:
        return None

    visit_dt = parse_date_only(visit_date)
    if not visit_dt:
        return None

    schedule_info = VACCINE_FLOW.get(current_schedule)
    if not schedule_info:
        return None

    days_until_next = schedule_info.get("days_until_next")
    if days_until_next is None:
        return None

    return visit_dt + timedelta(days=days_until_next)


def calculate_test_due_datetime(current_schedule, schedule_changed_at):
    if not current_schedule or not schedule_changed_at:
        return None

    base_time = parse_datetime(schedule_changed_at)
    if not base_time:
        return None

    delay_seconds = TEST_DELAY_MAP.get(current_schedule)
    if delay_seconds is None:
        return None

    return base_time + timedelta(seconds=delay_seconds)


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
# EMAIL CONTENT
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

This email will start to notify you for next vaccination.

Thank you and keep safe.

Care4Core Vaccination Reminder System
""".strip()


def build_reminder_email(name, guardian_name, current_schedule, next_schedule, next_vaccines, next_vaccination_date):
    vaccines_text = "\n".join(f"- {v}" for v in next_vaccines) if next_vaccines else "No vaccine listed."

    return f"""
Dear {guardian_name if guardian_name else "Parent/Guardian"},

Good day!

This is a friendly reminder regarding the next vaccination schedule for {name}.

Current completed schedule:
{current_schedule}

Next vaccination schedule:
{next_schedule if next_schedule else "No next schedule available"}

Next vaccination date:
{next_vaccination_date}

Recommended vaccine(s) for the next visit:
{vaccines_text}

Please visit your nearest health center on or before the expected vaccination date.

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
# MAIN PROCESS
# =========================
def process_patients():
    conn = None

    try:
        rows = fetch_csv(CSV_URL)

        conn = connect_db()
        cursor = conn.cursor()

        for row in rows:
            timestamp = get_value(row, "Timestamp")
            password = get_value(row, "Password")
            consent = get_value(
                row,
                'Bilang pagsangod sa Data Privacy Act of 2012 Nauunawaan ko na ang aking datos ay gagamitin sa pamamagitn ng pag-click sa "Sang-ayon"',
                'Bilang pagsangod sa Data Privacy Act of 2012 Nauunawaan ko na ang aking datos ay gagamitin Sa pamamagitan ng pag-click sa "Sang-ayon"'
            )
            admission_type = get_value(row, "Admission Type")
            name = get_value(row, "Pangalan (Apelyido, Pangalan, M.I.)")
            age_text = get_value(row, "Edad")
            guardian = get_value(row, "Pangalan ng Magulang o Tagapangalaga")
            contact = get_value(row, "Contact Number (ex. 9649127322)")
            email = get_value(row, "Email Address", "Email address")
            vaccine = get_value(row, "Klase ng Bakuna")
            schedule_raw = get_value(row, "Iskedyul ng mga Bakuna")
            visit = get_value(row, "Petsa ng Pagbisita")

            if not timestamp:
                continue

            try:
                age = int(age_text)
            except (TypeError, ValueError):
                age = None

            normalized_schedule = normalize_schedule(schedule_raw)
            patient = get_patient_by_timestamp(cursor, timestamp)

            if patient is None:
                detected_time = now_str()

                insert_patient(cursor, (
                    timestamp,
                    password,
                    consent,
                    admission_type,
                    name,
                    age,
                    guardian,
                    contact,
                    email,
                    vaccine,
                    normalized_schedule,
                    visit,
                    None,
                    0,
                    detected_time
                ))

                stored_schedule = normalized_schedule
                last_emailed_schedule = None
                initial_email_sent = 0
                schedule_changed_at = detected_time

                print(f"New patient added: {name}")

            else:
                _, stored_schedule, last_emailed_schedule, initial_email_sent, schedule_changed_at = patient

                schedule_to_store = normalized_schedule if normalized_schedule else stored_schedule

                update_patient_info(
                    cursor,
                    timestamp,
                    password,
                    consent,
                    admission_type,
                    name,
                    age,
                    guardian,
                    contact,
                    email,
                    vaccine,
                    schedule_to_store,
                    visit
                )

                if normalized_schedule and normalized_schedule != stored_schedule:
                    new_changed_at = now_str()
                    update_schedule_and_time(cursor, timestamp, normalized_schedule, new_changed_at)
                    print(f"Schedule changed for {name}: {stored_schedule} -> {normalized_schedule}")
                    stored_schedule = normalized_schedule
                    schedule_changed_at = new_changed_at

                normalized_schedule = schedule_to_store

            if email and initial_email_sent == 0:
                try:
                    subject = f"Care4Core Registration Confirmation for {name}"
                    body = build_initial_email(name, guardian, normalized_schedule, visit)
                    send_email(email, subject, body)
                    mark_initial_email_sent(cursor, timestamp)
                    initial_email_sent = 1
                    print(f"Initial email sent to {name} ({email})")
                except Exception as e:
                    print(f"Failed to send initial email to {name} ({email}): {e}")

            if not email:
                print(f"Skipped reminder for {name}: no email address.")
                continue

            if not normalized_schedule:
                print(f"Skipped reminder for {name}: schedule not recognized.")
                continue

            if normalized_schedule == last_emailed_schedule:
                print(f"No reminder for {name}: already emailed for schedule '{normalized_schedule}'.")
                continue

            next_info = VACCINE_FLOW.get(normalized_schedule)
            if not next_info:
                print(f"Skipped reminder for {name}: no vaccine flow found.")
                continue

            next_schedule = next_info.get("next_schedule")
            next_vaccines = next_info.get("next_vaccines", [])

            if not next_schedule:
                print(f"No reminder for {name}: no next schedule available.")
                update_last_emailed_schedule(cursor, timestamp, normalized_schedule)
                continue

            next_vaccination_dt = calculate_next_vaccination_date(normalized_schedule, visit)
            if not next_vaccination_dt:
                print(f"Skipped reminder for {name}: could not calculate next vaccination date.")
                continue

            if TEST_MODE:
                due_time = calculate_test_due_datetime(normalized_schedule, schedule_changed_at)
                if not due_time:
                    print(f"Skipped reminder for {name}: could not calculate test due time.")
                    continue

                if datetime.now() < due_time:
                    remaining = int((due_time - datetime.now()).total_seconds())
                    print(f"Waiting for reminder of {name}: {remaining} second(s) remaining.")
                    continue
            else:
                if datetime.now().date() < next_vaccination_dt.date():
                    print(f"Not yet time to remind {name}. Next vaccination date is {format_vaccination_date(next_vaccination_dt)}.")
                    continue

            try:
                subject = f"Vaccination Reminder for {name}"
                body = build_reminder_email(
                    name=name,
                    guardian_name=guardian,
                    current_schedule=normalized_schedule,
                    next_schedule=next_schedule,
                    next_vaccines=next_vaccines,
                    next_vaccination_date=format_vaccination_date(next_vaccination_dt)
                )
                send_email(email, subject, body)
                update_last_emailed_schedule(cursor, timestamp, normalized_schedule)
                print(f"Reminder email sent to {name} ({email})")
            except Exception as e:
                print(f"Failed to send reminder email to {name} ({email}): {e}")

        conn.commit()

    except Exception as e:
        print("Error while processing patients:", e)

    finally:
        if conn:
            conn.close()


# =========================
# MAIN
# =========================
def main():
    create_table()
    ensure_columns_exist()

    print("Monitoring Google Sheet for new patients and reminders...")
    print(f"TEST_MODE = {TEST_MODE}")
    print("-" * 60)

    while True:
        process_patients()
        print("-" * 60)
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()