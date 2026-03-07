## Just combines all datasets no normalizing done yet

import pandas as pd
import json
import os

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


def combine():

    print("Loading datasets")

    enron = load_csv("enron.csv", "enron")
    nazario = load_csv("nazario.csv", "nazario")
    github = load_json("github_phishing_emails.json", "github")


    combined = pd.concat(
        [enron, nazario, github],
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