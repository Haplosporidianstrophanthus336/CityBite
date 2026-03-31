"""
CityBite model evaluation utilities — RMSE for ALS, F1 for sentiment.

Can be imported by als_train.py / sentiment.py, or run directly as a
standalone evaluation report against a seeded database.

Standalone usage:
    python ml/evaluate.py --mode als --input data/processed/
    python ml/evaluate.py --mode sentiment --input data/processed/
    python ml/evaluate.py --mode all --input data/processed/
"""

import argparse
import os

from dotenv import load_dotenv
import pandas as pd

load_dotenv()

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_LOCAL_DB = os.path.join(_PROJECT_ROOT, "data", "citybite_local.db")


# ---------------------------------------------------------------------------
# ALS — RMSE evaluation
# ---------------------------------------------------------------------------

def evaluate_als_rmse(input_path: str) -> float:
    """
    Train a minimal ALS model on sample data and return test RMSE.

    Uses local PySpark so no cluster is required.  Mirrors the logic in
    als_train.py but is streamlined for a quick evaluation pass.
    """
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder
        .appName("CityBite-EvalALS")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    path = input_path.rstrip("/") + "/reviews_enriched"
    df = spark.read.parquet(path).select(
        "user_id", "business_id", F.col("stars").cast("float").alias("stars")
    )

    user_idx = StringIndexer(inputCol="user_id",     outputCol="user_idx",     handleInvalid="skip")
    biz_idx  = StringIndexer(inputCol="business_id", outputCol="business_idx", handleInvalid="skip")
    pipeline = Pipeline(stages=[user_idx, biz_idx])
    indexed  = pipeline.fit(df).transform(df)

    matrix = (
        indexed
        .withColumn("user_idx",     F.col("user_idx").cast("int"))
        .withColumn("business_idx", F.col("business_idx").cast("int"))
        .select("user_idx", "business_idx", "stars")
    )

    train_df, test_df = matrix.randomSplit([0.8, 0.2], seed=42)

    als = ALS(
        rank=20, maxIter=10, regParam=0.1,
        userCol="user_idx", itemCol="business_idx", ratingCol="stars",
        coldStartStrategy="drop", nonnegative=True,
    )
    model = als.fit(train_df)

    preds = model.transform(test_df).filter(F.col("prediction").isNotNull())
    rmse  = RegressionEvaluator(
        metricName="rmse", labelCol="stars", predictionCol="prediction"
    ).evaluate(preds)

    spark.stop()
    return rmse


# ---------------------------------------------------------------------------
# Sentiment — F1 evaluation
# ---------------------------------------------------------------------------

def evaluate_sentiment_f1(input_path: str) -> tuple[float, float]:
    """
    Train a TF-IDF + LogisticRegression classifier on sample data.

    Returns:
        (accuracy, weighted_f1)
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, f1_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline

    path = input_path.rstrip("/") + "/reviews_enriched"
    reviews = pd.read_parquet(path, columns=["text", "stars"])

    labeled = reviews[reviews["stars"] != 3].copy()
    labeled["label"] = (labeled["stars"] >= 4).astype(int)
    if len(labeled) > 200_000:
        labeled = labeled.sample(200_000, random_state=42)

    X = labeled["text"].fillna("").values
    y = labeled["label"].values

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    clf = Pipeline([
        ("tfidf", TfidfVectorizer(max_features=10_000, ngram_range=(1, 2))),
        ("clf",   LogisticRegression(max_iter=1000)),
    ])
    clf.fit(X_train, y_train)
    y_pred = clf.predict(X_test)

    return accuracy_score(y_test, y_pred), f1_score(y_test, y_pred, average="weighted")


# ---------------------------------------------------------------------------
# Precision@k — offline metric for ALS recommendations
# ---------------------------------------------------------------------------

def precision_at_k(db_url: str, k: int = 10, threshold: float = 4.0) -> float:
    """
    Compute precision@k for stored ALS recommendations.

    A recommendation is considered "relevant" if its predicted_rating >= threshold.
    This is an optimistic offline metric — it evaluates predicted scores, not
    held-out ground truth — and is most useful for comparing model versions.

    Args:
        db_url:    SQLAlchemy connection string pointing to a seeded database.
        k:         Rank cutoff (default 10, matching TOP_N_RECS in als_train.py).
        threshold: Minimum predicted rating to count as relevant (default 4.0).

    Returns:
        Mean precision@k across all users who have recommendations.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(db_url)
    with engine.connect() as conn:
        recs = pd.read_sql(
            text(
                "SELECT user_id, predicted_rating, rank "
                "FROM als_recommendations WHERE rank <= :k"
            ),
            conn,
            params={"k": k},
        )

    if recs.empty:
        return 0.0

    recs["relevant"] = (recs["predicted_rating"] >= threshold).astype(int)
    per_user = recs.groupby("user_id")["relevant"].mean()
    return float(per_user.mean())


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="CityBite model evaluation report")
    parser.add_argument(
        "--mode",
        choices=["als", "sentiment", "all"],
        default="all",
        help="Which evaluation to run",
    )
    parser.add_argument(
        "--input",
        default="data/processed/",
        help="Path to processed/ directory (containing reviews_enriched/)",
    )
    parser.add_argument(
        "--db",
        default=f"sqlite:///{_LOCAL_DB}",
        help="SQLAlchemy DB URL for precision@k (defaults to local SQLite)",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=10,
        help="Rank cutoff for precision@k (default 10)",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("CityBite Model Evaluation Report")
    print("=" * 60)

    if args.mode in ("als", "all"):
        print("\n[ALS] Computing test-set RMSE ...")
        rmse = evaluate_als_rmse(args.input)
        print(f"  RMSE = {rmse:.4f}  (target < 1.50)")
        status = "PASS" if rmse < 1.5 else "FAIL"
        print(f"  Status: {status}")

        print(f"\n[ALS] Computing precision@{args.k} from database ...")
        pk = precision_at_k(args.db, k=args.k)
        print(f"  Precision@{args.k} = {pk:.4f}")

    if args.mode in ("sentiment", "all"):
        print("\n[Sentiment] Training TF-IDF + LR classifier ...")
        acc, f1 = evaluate_sentiment_f1(args.input)
        print(f"  Accuracy = {acc:.4f}")
        print(f"  F1       = {f1:.4f}  (target >= 0.80)")
        status = "PASS" if f1 >= 0.80 else "FAIL"
        print(f"  Status: {status}")

    print("\n" + "=" * 60)
    print("Done.")


if __name__ == "__main__":
    main()
