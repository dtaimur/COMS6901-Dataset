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


def combine():

    print("Loading datasets")

    enron = load_csv("enron_data_fraud_labeled.csv", "enron")
    nazario = load_csv("nazario.csv", "nazario")
    github = load_json("github_phishing_emails.json", "github")
    meajor = load_csv("meajor.csv", "meajor")
    phishing_pot = load_eml_files("phishing_pot/email", "phishing_pot", "phishing")
    nazario_monkey = load_mbox("nazario_spf", "nazario_monkey", "phishing")
    scraped = load_eml_zips()

    datasets = [enron, nazario, github, meajor, phishing_pot, nazario_monkey]

    # datasets = [github, meajor, phishing_pot, nazario_monkey]
    if not scraped.empty:
        scraped = anonymize_scraped(scraped)
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


