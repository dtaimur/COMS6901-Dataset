import os
import subprocess
import requests
import shutil

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)


def download_kaggle_dataset(dataset_name):

    print(f"Downloading {dataset_name} from Kaggle...")

    subprocess.run(
        [
            "kaggle",
            "datasets",
            "download",
            "-d",
            dataset_name,
            "-p",
            RAW_DIR,
            "--unzip",
        ],
        check=True,
    )


def download_github_file(url, filename):

    print(f"Downloading {filename} from GitHub...")

    response = requests.get(url)
    response.raise_for_status()

    filepath = os.path.join(RAW_DIR, filename)

    with open(filepath, "wb") as f:
        f.write(response.content)

def download_zenodo_dataset(url, filename):

    print(f"Downloading {filename} from Zenodo...")

    response = requests.get(url)
    response.raise_for_status()

    filepath = os.path.join(RAW_DIR, filename)

    with open(filepath, "wb") as f:
        f.write(response.content)


def rename_file(original, new_name):

    original_path = os.path.join(RAW_DIR, original)
    new_path = os.path.join(RAW_DIR, new_name)

    if os.path.exists(original_path):
        shutil.move(original_path, new_path)
        print(f"Renamed {original} → {new_name}")
    else:
        print(f"Warning: {original} not found.")


if __name__ == "__main__":

    download_kaggle_dataset(
        "rohansood98/phishing-email-dataset-nazario-5-and-trec07"
    )

    rename_file(
        "Nazario_5.csv",
        "nazario.csv"
    )

    download_kaggle_dataset(
        "advaithsrao/enron-fraud-email-dataset"
    )

    rename_file(
        "emails.csv",
        "enron.csv"
    )

    github_url = (
        "https://raw.githubusercontent.com/dtaimur/PhishingSpamDataSet/refs/heads/main/1_DataSet/cleaned_emails.json"
    )

    download_github_file(
        github_url,
        "github_phishing_emails.json"
    )

    zenodo_url = (
        "https://zenodo.org/records/18471483/files/meajor_cleaned_preprocessed.csv"
    )

    download_zenodo_dataset(
        zenodo_url,
        "meajor.csv"
    )

    extra_file = os.path.join(RAW_DIR, "email_text.csv")

    if os.path.exists(extra_file):
        os.remove(extra_file)


    print("\nAll datasets downloaded successfully.")

    #TODO: add meajor corpus