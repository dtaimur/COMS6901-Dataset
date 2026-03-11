import pandas as pd
import json
import os
import re
from urllib.parse import urlparse

INPUT_FILE = "data/processed/combined_raw_dataset.csv"
OUTPUT_FILE = "data/processed/normalized_dataset.csv"

os.makedirs("data/processed", exist_ok=True)


def extract_domain(email):
    if pd.isna(email):
        return None
    if "@" in str(email):
        return email.split("@")[-1].lower()
    return None


def extract_urls(text):
    if pd.isna(text):
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
        except:
            return None
        if l == 1:
            return "phishing"
        elif l == 0:
            return "legitimate"

    return None


def normalize():
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    # Clean column names
    df.columns = df.columns.str.strip()

    # Merge duplicate columns if they exist
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

    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated()]

    # Create unified email_text column
    if "body" in df.columns and "message" in df.columns:
        df["email_text"] = df["body"].combine_first(df["message"])
    elif "body" in df.columns:
        df["email_text"] = df["body"]
    else:
        df["email_text"] = df["message"]


    df["sender_domain"] = df["sender"].apply(extract_domain)
    df["extracted_urls"] = df["email_text"].apply(extract_urls)
    df["url_domains"] = df["extracted_urls"].apply(get_domains_from_urls)
    df["num_urls"] = df["extracted_urls"].apply(len)
    df["has_ip_url"] = df["extracted_urls"].apply(contains_ip_url)
    df["email_length"] = df["email_text"].astype(str).apply(len)
    df["num_exclamation_marks"] = df["email_text"].astype(str).str.count("!")
    df["num_links_in_body"] = df["email_text"].astype(str).str.count("http")
    df["is_html_email"] = df["email_text"].astype(str).str.contains("<html|<body|<a", case=False).astype(int)

    if "human evaluated emotion" in df.columns:
        df["emotion_human"] = df["human evaluated emotion"]

    if "llm detected emotion" in df.columns:
        df["emotion_llm"] = df["llm detected emotion"]

    if "motivation" in df.columns:
        df["motivation"] = df["motivation"]

    # Normalize labels
    df["normalized_label"] = df.apply(normalize_label, axis=1)

    #Replace missing labels with deafult "legitimate"
    df["normalized_label"] = df["normalized_label"].fillna("legitimate")

    # Create numeric label_id for ML
    label_map = {"legitimate": 0, "spam": 1, "phishing": 2}
    df["label_id"] = df["normalized_label"].map(label_map)

    final_columns = [
        "email_text",
        "subject",
        "sender",
        "sender_domain",
        "receiver",
        "date",
        "normalized_label",
        "label_id",
        "source",
        "year",
        "num_urls",
        "has_ip_url",
        "email_length",
        "num_exclamation_marks",
        "num_links_in_body",
        "is_html_email",
        "url_domains",
        "emotion_human",
        "emotion_llm",
        "motivation"
    ]

    final_columns = [c for c in final_columns if c in df.columns]
    df = df[final_columns]

    # Drop rows with no email text
    df = df.dropna(subset=["email_text"])

    print("Final dataset size:", len(df))
    df.to_csv(OUTPUT_FILE, index=False)

    #print("Unique categorical labels:", df["normalized_label"].unique())
    #print("Unique numeric labels:", df["label_id"].unique())

    print("Saved normalized dataset:", OUTPUT_FILE)


if __name__ == "__main__":
    normalize()