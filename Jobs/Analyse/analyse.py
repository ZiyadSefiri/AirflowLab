# sentiment_analysis_pipeline.py (Updated for Compatibility)

import ast
from datetime import datetime
from pathlib import Path

import pandas as pd
from transformers import pipeline

# -----------------------------
# Config - ALIGNED WITH TRANSFORM SCRIPT
# -----------------------------
TEXT_COL = "comments"  # Ensure your JSON/first script includes this
ID_COL = "id"         # Changed from 'video_id' to 'id' to match standard YT JSON

MODEL_NAME = "cardiffnlp/twitter-roberta-base-sentiment-latest"
BATCH_SIZE = 32
MAX_LENGTH = 256

def ts():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def parse_list(x):
    """Ensure the column is parsed into a Python list"""
    if isinstance(x, (list, pd.Series)):
        return x
    if pd.isna(x) or x is None:
        return []
    try:
        # If it was saved as a string representation of a list
        return ast.literal_eval(x)
    except Exception:
        return [str(x)]

def main():
    # -----------------------------
    # Paths (Kept as per your directory structure)
    # -----------------------------
    base_path = Path(__file__).resolve().parent
    input_file = base_path / "input" / "video_details_transformed.parquet"
    output_dir = base_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    # -----------------------------
    # Load data
    # -----------------------------
    if not input_file.exists():
        raise FileNotFoundError(f"File not found: {input_file}. Check your Docker mounts!")

    df = pd.read_parquet(input_file)

    # Safety check for the column names
    if TEXT_COL not in df.columns:
        # If the first script renamed 'id' to something else, we handle it here
        print(f"Available columns: {df.columns.tolist()}")
        raise ValueError(f"The column '{TEXT_COL}' was not found in the Parquet file.")

    # -----------------------------
    # Normalize comments
    # -----------------------------
    # We use the ID_COL ('id') from your first script to keep the relationship
    df["_list"] = df[TEXT_COL].apply(parse_list)
    
    # Create a long-form dataframe where each row is one comment
    # and keep 'id' as the reference key
    sentiment_df = (
        df[[ID_COL, "_list"]]
        .explode("_list")
        .rename(columns={"_list": "text"})
    )
    
    # Clean text: remove empty strings and nulls
    sentiment_df["text"] = sentiment_df["text"].astype(str).str.strip()
    sentiment_df = sentiment_df[sentiment_df["text"].str.len() > 0].reset_index(drop=True)

    # -----------------------------
    # Load sentiment model
    # -----------------------------
    clf = pipeline(
        "sentiment-analysis",
        model=MODEL_NAME,
        tokenizer=MODEL_NAME,
        truncation=True,
    )

    # -----------------------------
    # Batch predictions
    # -----------------------------
    texts = sentiment_df["text"].tolist()
    preds = []

    print(f"Starting sentiment analysis on {len(texts)} comments...")

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i : i + BATCH_SIZE]
        preds.extend(
            clf(
                batch,
                batch_size=BATCH_SIZE,
                truncation=True,
                max_length=MAX_LENGTH,
            )
        )

    sentiment_df["sentiment_label"] = [p["label"] for p in preds]
    sentiment_df["sentiment_score"] = [float(p["score"]) for p in preds]

    # -----------------------------
    # Save results
    # -----------------------------
    output_file = output_dir / f"sentiment_results_{ts()}.csv"
    sentiment_df.to_csv(output_file, index=False)

    print(f"Saved sentiment results to: {output_file}")

if __name__ == "__main__":
    main()