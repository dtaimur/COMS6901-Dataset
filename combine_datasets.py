## Just combines all datasets no normalizing done yet. Some pre-processing on SPF extraction and spam anonymization.

import pandas as pd
import json
import os
import re
import mailbox
import zipfile
import email
import hashlib

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

os.makedirs(PROCESSED_DIR, exist_ok=True)

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

    # Email addresses
    text = re.sub(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b',
        PLACEHOLDERS["email"],
        text
    )

    # Phone numbers
    text = re.sub(
        r'(\+?\d{1,2}[\s\-\.]?)?(\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4})',
        PLACEHOLDERS["phone"],
        text
    )

    # Social Security Numbers
    text = re.sub(
        r'\b\d{3}-\d{2}-\d{4}\b',
        PLACEHOLDERS["ssn"],
        text
    )

    # Credit card numbers
    text = re.sub(
        r'\b(?:\d[ -]*?){13,16}\b',
        PLACEHOLDERS["credit_card"],
        text
    )

    # Physical addresses
    text = re.sub(
        r'\b\d{1,5}\s+[A-Za-z0-9\s,]+\s(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|Court|Ct)\b',
        PLACEHOLDERS["address"],
        text,
        flags=re.IGNORECASE
    )

    # URLs
    text = re.sub(
        r'https?://[^\s]+',
        PLACEHOLDERS["url"],
        text
    )

    # Greetings (e.g., "Dear John,")
    text = re.sub(
        r'\b(Dear|Hello|Hi|Greetings)\s+[A-Z][a-z]+\b',
        r'\1 ' + PLACEHOLDERS["name"],
        text
    )

    # Email signatures
    text = re.sub(
        r'(Best regards|Sincerely|Regards|Kind regards|Thanks)[\s\S]{0,200}',
        PLACEHOLDERS["signature"],
        text,
        flags=re.IGNORECASE
    )

    return text


def anonymize_headers(headers):
    if pd.isna(headers):
        return ""
    return anonymize_text(headers)


def anonymize_dataset(df):
    df = df.copy()

    text_columns = [
        "from", "to", "subject", "reply_to",
        "return_path", "headers", "body",
        "authentication_results"
    ]

    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(anonymize_text)

    return df

def get_header(msg, *keys):
    for key in keys:
        try:
            val = msg.get(key)
            if val:
                return val
        except Exception:
            continue
    return None


def extract_spf_fallback(msg):
    try:
        spam_summary = msg.get("X-Spam-Summary", "")
        match = re.search(r"SPF:(\w+)", spam_summary)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def get_body(msg):
    try:
        if msg.is_multipart():
            html_fallback = None
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    payload = part.get_payload(decode=True)
                    if payload:
                        for encoding in ("utf-8", "latin-1", "windows-1252"):
                            try:
                                return payload.decode(encoding)
                            except UnicodeDecodeError:
                                continue
                elif part.get_content_type() == "text/html" and html_fallback is None:
                    payload = part.get_payload(decode=True)
                    if payload:
                        for encoding in ("utf-8", "latin-1", "windows-1252"):
                            try:
                                html_fallback = payload.decode(encoding)
                                break
                            except UnicodeDecodeError:
                                continue
            return html_fallback
        else:
            payload = msg.get_payload(decode=True)
            if payload:
                for encoding in ("utf-8", "latin-1", "windows-1252"):
                    try:
                        return payload.decode(encoding)
                    except UnicodeDecodeError:
                        continue
    except Exception:
        pass
    return None


def extract_record(msg, source, label, source_file=None):
    return {
        "source": source,
        "source_file": source_file,
        "label": label,
        "from": get_header(msg, "From", "X-Original-From", "X-Sender"),
        "to": get_header(msg, "To", "X-Original-To", "Delivered-To"),
        "subject": get_header(msg, "Subject"),
        "date": get_header(msg, "Date"),
        "message_id": get_header(msg, "Message-ID"),
        "return_path": get_header(msg, "Return-Path"),
        "reply_to": get_header(msg, "Reply-To"),
        "received_spf": get_header(msg, "Received-SPF", "X-Received-SPF", "X-SPF") or extract_spf_fallback(msg),
        "authentication_results": get_header(msg, "Authentication-Results", "X-Authentication-Results"),
        "headers": str(dict(msg.items())),
        "body": get_body(msg),
    }

def anonymize_email(addr):
    if pd.isna(addr) or addr == "":
        return ""
    addr = str(addr)
    # extract email if in "Name <email@domain.com>" format
    match = re.search(r'<([^>]+)>', addr)
    address = match.group(1) if match else addr.strip()
    # hash the local part, keep domain
    if "@" in address:
        local, domain = address.split("@", 1)
        hashed = hashlib.sha256(local.encode()).hexdigest()[:10]
        return f"{hashed}@{domain.lower()}"
    return hashlib.sha256(address.encode()).hexdigest()[:10]

def anonymize_scraped(df):
    df = df.copy()
    df["from"] = df["from"].apply(anonymize_email)
    df["to"] = df["to"].apply(anonymize_email)
    return df


def load_eml_files(subdir, source, label):
    input_dir = os.path.join(RAW_DIR, subdir)
    records = []
    failed = 0

    for filename in os.listdir(input_dir):
        if not filename.endswith(".eml"):
            continue
        filepath = os.path.join(input_dir, filename)
        try:
            with open(filepath, "rb") as f:
                msg = email.message_from_binary_file(f)
            records.append(extract_record(msg, source, label, filename))
        except Exception as e:
            failed += 1
            print(f"  Warning: failed to parse {filename}: {e}")

    print(f"Loaded {len(records)} emails from {subdir} ({failed} failed)")
    return pd.DataFrame(records)


def load_mbox(subdir, source, label):
    input_dir = os.path.join(RAW_DIR, subdir)
    records = []
    failed = 0

    for filename in sorted(os.listdir(input_dir)):
        if filename.endswith(".tmp"):
            continue
        filepath = os.path.join(input_dir, filename)
        try:
            mbox = mailbox.mbox(filepath)
        except Exception as e:
            print(f"  Warning: could not open {filename}: {e}")
            continue
        for msg in mbox:
            try:
                records.append(extract_record(msg, source, label, filename))
            except Exception as e:
                failed += 1
                print(f"  Warning: failed to parse message in {filename}: {e}")
        print(f"  {filename}: {len(mbox)} emails")

    print(f"Loaded {len(records)} emails from {subdir} ({failed} failed)")
    return pd.DataFrame(records)

def load_csv(file, source):

    path = os.path.join(RAW_DIR, file)

    df = pd.read_csv(path)

    df["source"] = source

    return df


def load_json(file, source):

    path = os.path.join(RAW_DIR, file)

    with open(path) as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    df["source"] = source

    return df

def load_eml_zips(source_name="scraped_spam"):
    spam_zip_dir = os.path.join(RAW_DIR, "spam_zips")

    if not os.path.exists(spam_zip_dir):
        print("No spam zip folder found.")
        return pd.DataFrame()

    all_rows = []

    for zip_file in os.listdir(spam_zip_dir):
        if not zip_file.endswith(".zip"):
            continue

        zip_path = os.path.join(spam_zip_dir, zip_file)

        print(f"Processing zip: {zip_file}")

        with zipfile.ZipFile(zip_path, 'r') as z:
            for file in z.namelist():
                if not file.endswith(".eml"):
                    continue

                try:
                    with z.open(file) as f:
                        msg = email.message_from_bytes(f.read())
                        record = extract_record(msg, source_name, "spam", file)
                        record["user"] = zip_file.replace(".zip", "")
                        all_rows.append(record)
                except Exception as e:
                    print(f"Error parsing {file}: {e}")

    df = pd.DataFrame(all_rows)

    print(f"Loaded {len(df)} emails from spam zips")

    return df

def load_anonymized_spam():
    path = "spam_emails/anonymized_spam.csv"
    if not os.path.exists(path):
        print("No anonymized spam dataset found.")
        return pd.DataFrame()

    df = pd.read_csv(path)
    df["source"] = "scraped_spam"
    return df

def load_manual_emails(subdir, source, label):
    input_dir = os.path.join(subdir)
    records = []
    failed = 0

    for filename in os.listdir(input_dir):
        if not filename.endswith(".eml"):
            continue
        filepath = os.path.join(input_dir, filename)
        try:
            with open(filepath, "rb") as f:
                msg = email.message_from_binary_file(f)
            records.append(extract_record(msg, source, label, filename))
        except Exception as e:
            failed += 1
            print(f"  Warning: failed to parse {filename}: {e}")

    print(f"Loaded {len(records)} emails from {subdir} ({failed} failed)")
    return pd.DataFrame(records)


def combine():

    print("Loading datasets")

    enron = load_csv("enron_data_fraud_labeled.csv", "enron")
    nazario = load_csv("nazario.csv", "nazario")
    github = load_json("github_phishing_emails.json", "github")
    meajor = load_csv("meajor.csv", "meajor")
    phishing_pot = load_eml_files("phishing_pot/email", "phishing_pot", "phishing")
    nazario_monkey = load_mbox("nazario_spf", "nazario_monkey", "phishing")
    rpuv_ham = load_eml_files("realprogrammersusevim_ham/dataset/1", "rpuv_email_dataset", "ham")
    scraped = load_anonymized_spam()
    manual = load_manual_emails("manual_emails", "Columbia", "legitimate")

    datasets = [enron, nazario, github, meajor, phishing_pot, nazario_monkey, rpuv_ham]

    # datasets = [github, meajor, phishing_pot, nazario_monkey, rpuv_ham]
     
    if not scraped.empty:
        datasets.append(scraped)
    
    if not manual.empty:
        datasets.append(scraped)

    combined = pd.concat(datasets,
        ignore_index=True,
        sort=False
    )

    output_path = os.path.join(
        PROCESSED_DIR,
        "combined_raw_dataset.csv"
    )

    combined.to_csv(output_path, index=False)

    print("Combined dataset saved to:")
    print(output_path)

    print("Total rows:", len(combined))
    print(f"Columns: {list(combined.columns)}")
    print(f"\nSPF coverage: {combined['received_spf'].notna().sum()} / {len(combined)} emails have SPF data")




if __name__ == "__main__":
    combine()


