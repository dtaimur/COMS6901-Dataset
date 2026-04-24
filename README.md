# Overview
This repository contains scripts for downloading, combining, and normalizing email data from four datasets:
-  [Phishing and Spam Email Dataset](https://github.com/HarmJ0y/PhishingSpamDataSet/tree/main)
-  [Meajor Corpus](https://zenodo.org/records/18471483?)
-  [Enron Fraud Email Dataset](https://www.kaggle.com/datasets/advaithsrao/enron-fraud-email-dataset)
-  [Nazario-5/TREC-07](https://www.kaggle.com/datasets/rohansood98/phishing-email-dataset-nazario-5-and-trec07)
-  [realprogrammersusevim](https://github.com/realprogrammersusevim/email-dataset.git)
-  [phishing_pot](https://github.com/rf-peixoto/phishing_pot.git)
-  [Nazario 2020-2025](https://monkey.org/~jose/phishing/)

# Instructions for email scraping 
1. Go to gmail account settings → see all settings → Forwarding and POP/IMAP
    - Click on Auto-Expunge on - Immediately update the server. (default) and save changes 
2. Open google account → manage your google account → security & sign in 
    - Turn on two factor authentication
3. Go to the following link: Create and manage your app passwords 
    - Create a new app password (remember this because it will be used later)
4. Run the following command in terminal python scrape_email.py
5. Enter the required input 
    - Email address 
    - App password (from step 3)
    - IMAP server (imap.gmail.com for gmail)
6. Output should be a zip file 

# Running Project
 To create the dateset run the run_project.sh script. It will output a normalized_dataset.csv located in the data/processed folder.

## Outputs
It ouputs a combined_raw_dataset.csv and normalized_dataset.csv; the latter has the following features (see presentation for further details: https://docs.google.com/presentation/d/1g9X00uMSGMQ-xaXw32FAZFgBlX_ioDF34XfvUkO8uT0/edit?usp=sharing):
- "email_text",
- "subject",
- "sender",
- "sender_domain",
- "receiver",
- "receiver_domain",
- "date",
- "normalized_label",
- "label_id",
- "source",
- "year",
- "num_urls",
- "has_ip_url",
- "email_length",
- "num_exclamation_marks",
- "num_links_in_body",
- "is_html_email",
- "url_domains",
- "url_length_max",
- "url_length_avg",
- "url_subdom_max",
- "url_subdom_avg",
- "attachment_count",
- "has_attachments",
- "content_types",
- "language",
- "human evaluated emotion",
- "llm detected emotion",
- "motivation"
- "urgency"

