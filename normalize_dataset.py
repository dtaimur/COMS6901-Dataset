import pandas as pd
import json
import os
import re
from urllib.parse import urlparse

INPUT_FILE = "data/processed/combined_raw_dataset.csv"
OUTPUT_FILE = "data/processed/normalized_dataset.csv"

os.makedirs("data/processed", exist_ok=True)

def extract_year(date_val):
    if pd.isna(date_val) or date_val == "":
        return 0
    try:
        cleaned = re.sub(r'\s+[A-Z]{2,4}$', '', str(date_val).strip())
        return pd.to_datetime(cleaned).year
    except:
        return 0

def extract_domain(email):
    if pd.isna(email) or email == "":
        return ""
    email = str(email)
    if "@" in email:
        return email.split("@")[-1].replace(">", "").replace("<", "").lower()
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

def extract_attachment_features(files):
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
        if t in ["phishing", "spam", "legitimate", "valid", "ham"]:
            return "legitimate" if t in ["valid", "ham"] else t

    if "label" in row.index and pd.notna(row["label"]):
        val = str(row["label"]).strip().lower()

        if val in ["phishing", "spam", "legitimate", "valid", "ham"]:
            return "legitimate" if val in ["valid", "ham"] else val

        try:
            l = int(float(val))
            if l == 1:
                return "phishing"
            elif l == 0:
                return "legitimate"
        except:
            pass

    return "unknown"

def compute_urgency(text, subject=""):
    if pd.isna(text):
        text = ""
    if pd.isna(subject):
        subject = ""

    combined = str(text).lower() + " " + str(subject).lower()
    urgent_keywords = [
        "urgent", "immediately", "asap", "action required", "verify", "suspend",
        "suspended", "click now", "last chance", "important", "attention",
        "password expires", "confirm", "update now", "limited time",
        "security alert", "unusual activity", "act now", "final notice"
    ]
    keyword_count = sum(1 for word in urgent_keywords if word in combined)
    exclam_count = combined.count("!")
    length = len(combined)

    score = 0
    score += keyword_count * 2
    score += exclam_count

    if length < 200 and keyword_count > 0:
        score += 2

    if score >= 6:
        return "very urgent"
    elif score >= 3:
        return "somewhat urgent"
    else:
        return "not urgent"

def normalize_email_text(text):
    if pd.isna(text):
        return ""
    return re.sub(r'\s+', ' ', str(text)).strip()

def normalize_urls(urls):
    if not urls:
        return []
    return [u.strip("\"'").lower() for u in urls]

def clean_email_address(addr):
    if pd.isna(addr):
        return ""
    return str(addr).strip().replace('"','').replace("'","").replace("<","").replace(">","").strip()

def normalize_spf(val):
    if pd.isna(val) or val == "":
        return "none"
    val = str(val).lower()
    if "pass" in val:
        return "pass"
    elif "fail" in val and "soft" in val:
        return "softfail"
    elif "fail" in val:
        return "fail"
    elif "neutral" in val:
        return "neutral"
    elif "none" in val:
        return "none"
    elif "permerror" in val or "temperror" in val:
        return "error"
    return "unknown"

def normalize():
    print("Loading dataset...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)

    # Clean column names
    df.columns = df.columns.str.strip()

    for col in ["label", "type"]:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].astype("object")

    # Merge duplicate columns 
    def merge_columns(df, primary, secondary):
        if primary in df.columns and secondary in df.columns:
            df[primary] = df[primary].combine_first(df[secondary])
            df = df.drop(columns=[secondary])
        return df

    df = merge_columns(df, "label", "Label")
    df = merge_columns(df, "type", "Type")
    df = merge_columns(df, "subject", "Subject")
    df = merge_columns(df, "body", "Body")
    df = merge_columns(df, "sender", "Sender")
    df = merge_columns(df, "file", "File")
    df = merge_columns(df, "source", "Source")
    df = merge_columns(df, "urls", "URL(s)")
    df = merge_columns(df, "year", "Year")
    df = merge_columns(df, "num_urls", "url_count")
    df = merge_columns(df, "sender", "from")
    df = merge_columns(df, "receiver", "to")
    df = merge_columns(df, "content_types", "content_type")
    df = merge_columns(df, "body", "message")
    df = merge_columns(df, "body", "text")

    if "received_spf" in df.columns:
        df["spf_result"] = df["received_spf"].apply(normalize_spf)
    else:
        df["spf_result"] = "none"

    # Create unified email_text column
    if "body" in df.columns and "message" in df.columns:
        df["email_text"] = df["body"].combine_first(df["message"])
    elif "body" in df.columns:
        df["email_text"] = df["body"]
    elif "message" in df.columns:
        df["email_text"] = df["message"]
    else:
        df["email_text"] = ""   
    df["email_text"] = df["email_text"].apply(normalize_email_text)

    # Lowercase column names, drop remaining duplicates
    df.columns = df.columns.str.lower()
    df = df.loc[:, ~df.columns.duplicated()] 

    # Fill missing text columns with empty string
    text_cols = ["subject", "sender", "receiver", "sender_domain", "receiver_domain", "email_text",
                 "motivation", "human evaluated emotion", "llm detected emotion", "file",
                 "source", "content_types", "language"]
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

    df["sender"] = df["sender"].apply(clean_email_address)
    df["receiver"] = df["receiver"].apply(clean_email_address)

    df["sender_domain"] = df.apply(
        lambda row: extract_domain(row["sender_domain"]) if row.get("sender_domain", "") != "" 
                    else extract_domain(row["sender"]),
        axis=1
    )

    df["receiver_domain"] = df.apply(
        lambda row: extract_domain(row["receiver_domain"]) if row.get("receiver_domain", "") != "" 
                    else extract_domain(row["receiver"]),
        axis=1
    )


    # Extract year from date if missing
    mask = df["year"] == 0
    if mask.any() and "date" in df.columns:
        df.loc[mask, "year"] = df.loc[mask, "date"].apply(extract_year)

    # Extract URLs and normalize
    df["extracted_urls"] = df["email_text"].apply(extract_urls)
    df["urls"] = df["urls"].apply(lambda x: x if isinstance(x, list) else extract_urls(str(x)))
    df["urls"] = df.apply(lambda row: list(set(row["extracted_urls"] + row["urls"])), axis=1)
    df["urls"] = df["urls"].apply(normalize_urls)
    df["url_domains"] = df["urls"].apply(get_domains_from_urls)
    df["num_urls"] = df["urls"].apply(len)

    # URL feature extraction
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
    df["is_html_email"] = (df["email_text"].astype(str).str.contains("<html|<body|<a", case=False) | df["content_types"].astype(str).str.contains("html", case=False)).astype(int)

    # Compute urgency
    df["urgency_level"] = df.apply(lambda row: compute_urgency(row["email_text"], row.get("subject", "")), axis=1)

    # Extract attachment features
    mask = df["attachment_count"] == 0
    if mask.any():
        results = df.loc[mask, "file"].apply(extract_attachment_features)
        df.loc[mask, "attachment_count"] = results.apply(lambda x: x[0])
        df.loc[mask, "has_attachments"] = results.apply(lambda x: float(x[1]))

    # Header-based (only fill missing)
    if "headers" in df.columns:
        header_attach = df["headers"].str.contains("filename=", case=False, na=False)
        header_count = df["headers"].str.count("filename=")

        df["has_attachments"] = df["has_attachments"].combine_first(header_attach.astype(float))
        df["attachment_count"] = df["attachment_count"].combine_first(header_count)

    # Fill annotation columns
    for col in ["human evaluated emotion", "llm detected emotion", "motivation"]:
        if col in df.columns:
            df[col] = df[col].fillna("")

    # Normalize labels
    df["normalized_label"] = df.apply(normalize_label, axis=1)
    severity_map = {"legitimate": 0, "spam": 1, "phishing": 2}
    df["label_severity"] = df["normalized_label"].map(severity_map)
    df = df.sort_values("label_severity", ascending=False)
    df = df.drop(columns=["label_severity"])
    df["label_id"] = df["normalized_label"].map({"legitimate":0, "spam":1, "phishing":2, 0.0: 0, 1.0: 1})
    df["normalized_label"] = df["normalized_label"].fillna("legitimate")
    df["label_id"] = df["label_id"].fillna(0)

    # Drop duplicate rows
    df = df.drop_duplicates(subset=["email_text", "subject", "sender", "year"])

    before = len(df)
    df = df[df["normalized_label"] != "unknown"]
    after = len(df)

    print("Removed rows with unknown labels:", before - after)

    # Final columns
    final_columns = [
        "email_text", "subject", "sender", "sender_domain", "receiver", "receiver_domain", "date",
        "normalized_label", "label_id", "source", "year",
        "num_urls", "has_ip_url", "email_length", "num_exclamation_marks",
        "num_links_in_body", "is_html_email", "url_domains",
        "url_length_max", "url_length_avg", "url_subdom_max", "url_subdom_avg",
        "attachment_count", "has_attachments",
        "content_types", "language",
        "human evaluated emotion", "llm detected emotion", "motivation", 
        "urgency_level", "spf_result",
        "headers"
    ]
    final_columns = [c for c in final_columns if c in df.columns]
    df = df[final_columns]

    # Example emails
    cols = ["subject", "sender", "sender_domain", "normalized_label", "label_id",
            "num_urls", "has_ip_url", "email_length", "num_exclamation_marks", "urgency_level"]
    legit_example = df[df["normalized_label"] == "legitimate"][cols].sample(1)
    phishing_example = df[df["normalized_label"] == "phishing"][cols].sample(1)

   # print("\nLEGITIMATE EMAIL EXAMPLE:\n", legit_example.to_string(index=False))
   # print("\nPHISHING EMAIL EXAMPLE:\n", phishing_example.to_string(index=False))
   # print("Final dataset size:", len(df))

    df.to_csv(OUTPUT_FILE, index=False)
    print("Saved normalized dataset:", OUTPUT_FILE)
    print(df.groupby("source")["normalized_label"].value_counts())

if __name__ == "__main__":
    normalize()