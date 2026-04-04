## Just combines all datasets no normalizing done yet

import pandas as pd
import json
import os
import zipfile
import email

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

os.makedirs(PROCESSED_DIR, exist_ok=True)


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

def parse_eml_file(file_bytes):
    msg = email.message_from_bytes(file_bytes)

    def get_body(msg):
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        return part.get_payload(decode=True).decode(errors="ignore")
                    except:
                        continue
        else:
            try:
                return msg.get_payload(decode=True).decode(errors="ignore")
            except:
                return ""
        return ""

    headers = dict(msg.items())

    return {
        "subject": msg.get("Subject", ""),
        "sender": msg.get("From", ""),
        "receiver": msg.get("To", ""),
        "date": msg.get("Date", ""),
        "body": get_body(msg),

        "headers": str(headers),

        "content_type": msg.get_content_type(),
        "mime_version": msg.get("Mime-Version", "")
    }

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
                        parsed = parse_eml_file(f.read())
                        parsed["source"] = source_name
                        parsed["user"] = zip_file.replace(".zip", "")
                        all_rows.append(parsed)
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
    scraped = load_eml_zips()

    datasets = [enron, nazario, github, meajor]

    if not scraped.empty:
        datasets.append(scraped)


    combined = pd.concat(
        datasets,
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




if __name__ == "__main__":
    combine()


