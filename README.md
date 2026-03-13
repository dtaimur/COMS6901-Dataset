# Overview
This repository contains scripts for downloading, combining, and normalizing email data from four datasets:
-  [Phishing and Spam Email Dataset](https://github.com/HarmJ0y/PhishingSpamDataSet/tree/main)
-  [Meajor Corpus](https://zenodo.org/records/18471483?)
-  [Enron Fraud Email Dataset](https://www.kaggle.com/datasets/advaithsrao/enron-fraud-email-dataset)
-  [Nazario-5/TREC-07](https://www.kaggle.com/datasets/rohansood98/phishing-email-dataset-nazario-5-and-trec07)

## Outputs
It ouputs a combined_raw_dataset.csv and normalized_dataset.csv; the latter has the following features:
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

