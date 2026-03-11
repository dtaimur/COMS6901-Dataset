import pandas as pd
import json
import os
import re
from urllib.parse import urlparse

INPUT_FILE = "data/processed/combined_raw_dataset.csv"
OUTPUT_FILE = "data/processed/normalized_dataset.csv"

os.makedirs("data/processed", exist_ok=True)

def extract_domain(email):
    if pd.isna(email) or email == "":
        return ""
    if "@" in str(email):
        return email.split("@")[-1].lower()
    return ""

def extract_urls(text):
    if pd.isna(text) or text == "":
        return []
    url_pattern = r'https?://[^\s]+'
    return re.findall(url_pattern, str(text))

def get_domains_from_urls(urls):
    domains = []
    for url in urls:
        try:
            parsed = urlparse(url)
            domains.append(parsed.netloc.lower())
        except:
            pass
    return domains

def contains_ip_url(urls):
    ip_pattern = r'\d+\.\d+\.\d+\.\d+'
    for url in urls:
        if re.search(ip_pattern, url):
            return 1
    return 0

def normalize_label(row):
    if "type" in row.index and pd.notna(row["type"]):
        t = str(row["type"]).strip().lower()
        if t == "phishing":
            return "phishing"
        elif t == "spam":
            return "spam"
        elif t == "valid":
            return "legitimate"

    if "label" in row.index and pd.notna(row["label"]):
        try:
            l = int(row["label"])
            if l == 1:
                return "phishing"
            elif l == 0:
                return "legitimate"
        except:
            pass

    return "legitimate"

def normalize():
    print("Loading dataset...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    # Clean column names
    df.columns = df.columns.str.strip()

    # Merge duplicate columns 
    def merge_columns(df, primary, secondary):
        if primary in df.columns and secondary in df.columns:
            df[primary] = df[primary].combine_first(df[secondary])
            df = df.drop(columns=[secondary])
        return df

    df = merge_columns(df, "subject", "Subject")
    df = merge_columns(df, "body", "Body")
    df = merge_columns(df, "sender", "Sender")
    df = merge_columns(df, "file", "File")
    df = merge_columns(df, "source", "Source")
    df = merge_columns(df, "urls", "URL(s)")
    df = merge_columns(df, "year", "Year")

    # Lowercase columns and remove any duplicates
    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated()]

    # Create unified email_text column
    if "body" in df.columns and "message" in df.columns:
        df["email_text"] = df["body"].combine_first(df["message"])
    elif "body" in df.columns:
        df["email_text"] = df["body"]
    elif "message" in df.columns:
        df["email_text"] = df["message"]
    else:
        df["email_text"] = ""

    # Fill missing text columns with empty string
    text_cols = ["subject", "sender", "receiver", "email_text", "motivation",
                 "human evaluated emotion", "llm detected emotion", "file", "source"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("")

    # Fill numeric columns with 0
    numeric_cols = ["year", "num_urls", "email_length", "num_exclamation_marks",
                    "num_links_in_body", "has_ip_url", "is_html_email"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df["sender_domain"] = df["sender"].apply(extract_domain)
    df["extracted_urls"] = df["email_text"].apply(extract_urls)
    df["url_domains"] = df["extracted_urls"].apply(get_domains_from_urls)
    df["num_urls"] = df["extracted_urls"].apply(len)
    df["has_ip_url"] = df["extracted_urls"].apply(contains_ip_url)
    df["email_length"] = df["email_text"].astype(str).apply(len)
    df["num_exclamation_marks"] = df["email_text"].astype(str).str.count("!")
    df["num_links_in_body"] = df["email_text"].astype(str).str.count("http")
    df["is_html_email"] = df["email_text"].astype(str).str.contains("<html|<body|<a", case=False).astype(int)

    # Fill annotation columns with empty string if missing
    for col in ["human evaluated emotion", "llm detected emotion", "motivation"]:
        if col in df.columns:
            df[col] = df[col].fillna("")

    df["normalized_label"] = df.apply(normalize_label, axis=1)
    df["label_id"] = df["normalized_label"].map({"legitimate":0, "spam":1, "phishing":2})

    # Ensure no NaNs in label columns
    df["normalized_label"] = df["normalized_label"].fillna("legitimate")
    df["label_id"] = df["label_id"].fillna(0)

    final_columns = [
        "email_text", "subject", "sender", "sender_domain", "receiver", "date",
        "normalized_label", "label_id", "source", "year",
        "num_urls", "has_ip_url", "email_length", "num_exclamation_marks",
        "num_links_in_body", "is_html_email",
        "url_domains",
        "human evaluated emotion", "llm detected emotion", "motivation"
    ]

    final_columns = [c for c in final_columns if c in df.columns]
    df = df[final_columns]
    
    print("Final dataset size:", len(df))
    df.to_csv(OUTPUT_FILE, index=False)
    print("Saved normalized dataset:", OUTPUT_FILE)


if __name__ == "__main__":
    normalize()