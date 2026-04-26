import pandas as pd
import os
import zipfile
import email
import re
import spacy
from email.header import decode_header

RAW_DIR = "data/raw/spam_zips"
OUTPUT_DIR = "spam_emails"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "anonymized_spam.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)


nlp = spacy.load("en_core_web_sm", disable=["parser", "tagger"])


PLACEHOLDERS = {
    "email": "example@gmail.com",
    "phone": "202-555-0123",
    "ssn": "123-45-6789",
    "credit_card": "4111-1111-1111-1111",
    "address": "123 Main St, Springfield, USA",
    "name": "John Doe",
    "org": "Example Organization",
    "url": "https://example.com"
}


SAFE_WORDS = {
    "Today", "University", "Monday", "Tuesday", "Wednesday",
    "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
}


def load_private_names(filepath="data/private_names.txt"):
    names = set()

    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                name = normalize_text(line).lower()
                if name:
                    names.add(name)

    return names


PRIVATE_NAMES = load_private_names()


def normalize_text(text):
    if not text:
        return ""

    text = str(text)

    
    text = text.replace("\r", " ")
    text = text.replace("\n", " ")
    text = text.replace("\t", " ")
    text = text.replace("\u00a0", " ")  
    text = text.replace("’", "'")

    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def strip_email_display_names(text):
    if not text:
        return ""

    return re.sub(r'\"?[^<"]+\"?\s*<([^>]+)>', r'\1', text)


def remove_private_names(text):
    text = normalize_text(text)

    for name in PRIVATE_NAMES:
        pattern = re.escape(name).replace(r'\ ', r'\s+')
        text = re.sub(pattern, PLACEHOLDERS["name"], text, flags=re.IGNORECASE)

    return text


def decode_email_text(text):
    if not text:
        return ""

    decoded_parts = decode_header(text)
    out = ""

    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            out += part.decode(encoding or "utf-8", errors="ignore")
        else:
            out += part

    return out


def anonymize_entities(text):
    doc = nlp(text)

    for ent in doc.ents:
        if ent.label_ == "PERSON":
            text = text.replace(ent.text, PLACEHOLDERS["name"])
        elif ent.label_ == "ORG":
            text = text.replace(ent.text, PLACEHOLDERS["org"])

    return text


def anonymize_names_regex(text):
    return text  


def anonymize_text(text):
    if pd.isna(text) or text is None:
        return ""

    text = decode_email_text(text)

    text = strip_email_display_names(text)

    text = normalize_text(text)

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

    text = re.sub(r'\b(dear|hello|hi|hey)\b[\s,:\-]*([A-Za-z][a-z]+)\b',r'\1 ' + PLACEHOLDERS["name"],text,
    flags=re.IGNORECASE)

    text = anonymize_entities(text)

    text = remove_private_names(text)

    text = cleanup_orphan_tokens(text)

    return text

def cleanup_orphan_tokens(text):
    text = re.sub(r'\b(an)\b', '', text)

    text = re.sub(r'\s+', ' ', text).strip()

    return text

def anonymize_headers(headers):
    if pd.isna(headers) or headers is None:
        return ""

    text = decode_email_text(str(headers))
    text = strip_email_display_names(text)
    text = normalize_text(text)

    # remove whitelist FIRST in headers too (structure heavy)
    text = remove_private_names(text)

    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
                  PLACEHOLDERS["email"], text)

    text = re.sub(r'\b\d{1,3}(?:\.\d{1,3}){3}\b', "0.0.0.0", text)

    text = re.sub(r'\b([a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}\b', "example.com", text)

    text = re.sub(r'<[^>]+>', "<message-id@example.com>", text)

    text = anonymize_entities(text)

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
        "headers": anonymize_headers(str(dict(msg.items()))),
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