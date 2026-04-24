import pandas as pd
import os
import zipfile
import email
import re

RAW_DIR = "data/raw/spam_zips"
OUTPUT_DIR = "spam_emails"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "anonymized_spam.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

PLACEHOLDERS = {
    "email": "example@gmail.com",
    "phone": "202-555-0123",
    "ssn": "123-45-6789",
    "credit_card": "4111-1111-1111-1111",
    "address": "123 Main St, Springfield, USA",
    "name": "John Doe",
    "url": "https://example.com",
    "signature": "Best regards,\nJohn Doe\nExample Corporation"
}


def anonymize_text(text):
    if pd.isna(text) or text is None:
        return ""

    text = str(text)

    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
                  PLACEHOLDERS["email"], text)

    text = re.sub(r'(\+?\d{1,2}[\s\-\.]?)?(\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4})',
                  PLACEHOLDERS["phone"], text)

    text = re.sub(r'\b\d{3}-\d{2}-\d{4}\b',
                  PLACEHOLDERS["ssn"], text)

    text = re.sub(r'\b(?:\d[ -]*?){13,16}\b',
                  PLACEHOLDERS["credit_card"], text)

    text = re.sub(r'https?://[^\s]+',
                  PLACEHOLDERS["url"], text)

    text = re.sub(r'\b(Dear|Hello|Hi)\s+[A-Z][a-z]+\b',
                  r'\1 ' + PLACEHOLDERS["name"], text)

    text = re.sub(r'(Best regards|Sincerely|Regards|Thanks|Best)[\s\S]{0,200}',
                  PLACEHOLDERS["signature"], text, flags=re.IGNORECASE)

    return text


def get_body(msg):
    try:
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        return payload.decode(errors="ignore")
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                return payload.decode(errors="ignore")
    except:
        pass
    return ""


def extract_record(msg, source_file):
    return {
        "source": "scraped_spam",
        "label": "spam",
        "from": anonymize_text(msg.get("From", "")),
        "to": anonymize_text(msg.get("To", "")),
        "subject": anonymize_text(msg.get("Subject", "")),
        "date": msg.get("Date", ""),
        "body": anonymize_text(get_body(msg)),
        "headers": anonymize_text(str(dict(msg.items()))),
        "source_file": source_file
    }


def process_spam_zips():
    all_rows = []

    for zip_file in os.listdir(RAW_DIR):
        if not zip_file.endswith(".zip"):
            continue

        print(f"Processing {zip_file}")

        with zipfile.ZipFile(os.path.join(RAW_DIR, zip_file)) as z:
            for file in z.namelist():
                if not file.endswith(".eml"):
                    continue

                try:
                    with z.open(file) as f:
                        msg = email.message_from_bytes(f.read())
                        all_rows.append(extract_record(msg, zip_file))
                except Exception as e:
                    print(f"Error: {e}")

    df = pd.DataFrame(all_rows)
    df.to_csv(OUTPUT_FILE, index=False)

    print(f"Saved anonymized dataset: {OUTPUT_FILE}")
    print(f"Total emails: {len(df)}")


if __name__ == "__main__":
    process_spam_zips()