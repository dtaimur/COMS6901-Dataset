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

def attachment_features(files):
    if pd.isna(files) or files == "":
        return 0, 0
    if isinstance(files, list):
        pass
    else:
        files = [f.strip().strip("'\"") for f in str(files).strip("[]").split(",") if f.strip()]
    count = len(files)
    if count > 0:
        return count, 1.0
    return count, 0.0


def extract_url_features(urls):
    lengths = [len(url) for url in urls]
    subdoms = [url.count(".") for url in urls]
    return {
        "url_length_max": max(lengths) if lengths else 0,
        "url_length_avg": sum(lengths)/len(lengths) if lengths else 0,
        "url_subdom_max": max(subdoms) if subdoms else 0,
        "url_subdom_avg": sum(subdoms)/len(subdoms) if subdoms else 0
    }

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
            l = int(float(row["label"]))
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
    # Lowercase columns and remove any duplicates
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated()]

    # Merge duplicate columns 
    def merge_columns(df, primary, secondary):
        if primary in df.columns and secondary in df.columns:
            df[primary] = df[primary].combine_first(df[secondary])
            df = df.drop(columns=[secondary])
        return df

    df = merge_columns(df, "urls", "url(s)")
    df = merge_columns(df, "num_urls", "url_count")

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
    text_cols = ["subject", "sender", "receiver", "sender_domain", "receiver_domain", "email_text", "motivation",
                 "human evaluated emotion", "llm detected emotion", "file", "source", "content_types", "language"]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna("")

    # Fill numeric columns with 0
    numeric_cols = ["year", "num_urls", "email_length", "num_exclamation_marks",
                    "num_links_in_body", "has_ip_url", "is_html_email", "url_length_max", 
                    "url_length_avg", "url_subdom_max", "url_subdom_avg", "has_attachments", "attachment_count"] 
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df["sender_domain"] = df.apply(
        lambda row: extract_domain(row["sender"]) if row.get("sender_domain", "") == "" else row["sender_domain"],
        axis=1
    )

    df["receiver_domain"] = df.apply(
        lambda row: extract_domain(row["receiver"]) if row.get("receiver_domain", "") == "" else row["receiver_domain"],
        axis=1
    )

    #Extracts year from date if year not present but date column is populated
    mask = df["year"] == 0
    if mask.any() and "date" in df.columns:
        df.loc[mask, "year"] = pd.to_datetime(df.loc[mask, "date"], errors="coerce").dt.year.fillna(0).astype(int)

    # Extract URLs from email text and combine with existing URL column
    df["extracted_urls"] = df["email_text"].apply(extract_urls)
    df["urls"] = df["urls"].apply(lambda x: x if isinstance(x, list) else extract_urls(str(x)))
    df["urls"] = df.apply(lambda row: list(set(row["extracted_urls"] + row["urls"])), axis=1)

    # Extract features from URLs if not already present in source dataset
    df["url_domains"] = df["urls"].apply(get_domains_from_urls)
    df["num_urls"] = df["urls"].apply(len)

    mask = df["url_length_max"] == 0
    if mask.any():
        url_features = df.loc[mask, "urls"].apply(extract_url_features)
        url_features_df = pd.DataFrame(url_features.tolist(), index=df.loc[mask].index)
        for col in ["url_length_max", "url_length_avg", "url_subdom_max", "url_subdom_avg"]:
            df.loc[mask, col] = url_features_df[col]
    
    df["has_ip_url"] = df["urls"].apply(contains_ip_url)
    df["email_length"] = df["email_text"].astype(str).apply(len)
    df["num_exclamation_marks"] = df["email_text"].astype(str).str.count("!")
    df["num_links_in_body"] = df["email_text"].astype(str).str.count("http")
    df["is_html_email"] = df["email_text"].astype(str).str.contains("<html|<body|<a", case=False).astype(int)

    # Processes file field to extract attachment features if not already present in source dataset
    mask = df["attachment_count"] == 0
    if mask.any():
        results = df.loc[mask, "file"].apply(attachment_features)
        df.loc[mask, "attachment_count"] = results.apply(lambda x: x[0])
        df.loc[mask, "has_attachments"] = results.apply(lambda x: float(x[1]))
    
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
    "email_text", "subject", "sender", "sender_domain", "receiver", "receiver_domain", "date",
    "normalized_label", "label_id", "source", "year",
    "num_urls", "has_ip_url", "email_length", "num_exclamation_marks",
    "num_links_in_body", "is_html_email", "url_domains",
    "url_length_max", "url_length_avg", "url_subdom_max", "url_subdom_avg",
    "attachment_count", "has_attachments",
    "content_types", "language",
    "human evaluated emotion", "llm detected emotion", "motivation"
    ]

    final_columns = [c for c in final_columns if c in df.columns]
    df = df[final_columns]
    
    print("Final dataset size:", len(df))
    df.to_csv(OUTPUT_FILE, index=False)
    print("Saved normalized dataset:", OUTPUT_FILE)


if __name__ == "__main__":
    normalize()