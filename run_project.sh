#!/bin/bash
set -e

#if [ ! -f ~/.kaggle/kaggle.json ]; then
#    echo "Kaggle API key not found!"
#    echo "Please place kaggle.json in ~/.kaggle/ before running."
#    exit 1
#fi

echo "Starting dataset creation pipeline..."

python3 download_datasets.py
python3 combine_datasets.py
python3 normalize_dataset.py

echo "Dataset pipeline completed successfully!"