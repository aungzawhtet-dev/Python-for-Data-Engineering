# clean_csv_module.py
from pathlib import Path
import pandas as pd

def clean_csv(input_path: str, output_path: str):
    input_path = Path(input_path)
    output_path = Path(output_path)

    # Ensure output folder exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_path)
    df = df.dropna(subset=['name', 'email'])
    df = df[df['email'].str.contains(r"@")]

    median_age = df['age'].median()
    df['age'] = df['age'].fillna(median_age)

    df.to_csv(output_path, index=False)
    print(f"Cleaned CSV saved to {output_path}")
