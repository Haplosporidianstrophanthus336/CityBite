"""
Generate synthetic 1% Yelp-format sample data for local development.

Usage:
    python data/sample/generate_sample.py
    python data/sample/generate_sample.py --businesses 500 --reviews 5000 --users 1000
"""

import argparse
import json
import math
import random
import string
from datetime import datetime, timedelta
from pathlib import Path

CITIES = [
    ("Phoenix", "AZ", 33.45, -112.07),
    ("Las Vegas", "NV", 36.17, -115.14),
    ("Charlotte", "NC", 35.23, -80.84),
]

CUISINES = [
    "Mexican", "American", "Italian", "Chinese", "Japanese",
    "Indian", "Thai", "Mediterranean", "Fast Food", "Pizza",
]

REVIEW_TEMPLATES = {
    5: [
        "Absolutely fantastic! Best {cuisine} in town.",
        "Amazing food, great service. Highly recommend!",
        "Loved every bite. Will definitely come back.",
    ],
    4: [
        "Pretty good {cuisine} spot. Food was tasty.",
        "Good food and nice atmosphere. Worth a visit.",
        "Solid choice for {cuisine}. Enjoyed the meal.",
    ],
    3: [
        "Decent place. Nothing special but okay.",
        "Average experience. Food was fine.",
        "It was okay. Might come back.",
    ],
    2: [
        "Disappointing. Expected more.",
        "Not great. Food was mediocre.",
        "Below average. Service was slow.",
    ],
    1: [
        "Terrible experience. Would not recommend.",
        "Awful food and poor service.",
        "Very disappointed. Do not come here.",
    ],
}


def _random_id(length: int = 22) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def _random_date(start_days_ago: int = 365 * 5) -> str:
    delta = timedelta(days=random.randint(0, start_days_ago))
    return (datetime.now() - delta).strftime("%Y-%m-%d %H:%M:%S")


def generate_businesses(n: int, city_name: str, state: str, base_lat: float, base_lng: float) -> list[dict]:
    businesses = []
    for _ in range(n):
        cuisine = random.choice(CUISINES)
        lat = base_lat + random.uniform(-0.3, 0.3)
        lng = base_lng + random.uniform(-0.3, 0.3)
        businesses.append({
            "business_id": _random_id(),
            "name": f"{random.choice(['The', 'Casa', 'Cafe', 'La'])} {cuisine} {random.choice(['Kitchen', 'Spot', 'Grill', 'Place'])}",
            "city": city_name,
            "state": state,
            "latitude": round(lat, 6),
            "longitude": round(lng, 6),
            "stars": round(random.uniform(1.0, 5.0), 1),
            "review_count": random.randint(5, 500),
            "is_open": random.choices([1, 0], weights=[0.85, 0.15])[0],
            "categories": f"{cuisine}, Restaurants",
        })
    return businesses


def generate_users(n: int) -> list[dict]:
    return [{"user_id": _random_id(), "name": f"User_{i}"} for i in range(n)]


def generate_reviews(businesses: list[dict], users: list[dict], n: int) -> list[dict]:
    reviews = []
    for _ in range(n):
        business = random.choice(businesses)
        user = random.choice(users)
        stars = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.10, 0.15, 0.35, 0.35])[0]
        cuisine = business["categories"].split(",")[0]
        text = random.choice(REVIEW_TEMPLATES[stars]).format(cuisine=cuisine)
        reviews.append({
            "review_id": _random_id(),
            "user_id": user["user_id"],
            "business_id": business["business_id"],
            "stars": stars,
            "date": _random_date(),
            "text": text,
            "useful": random.randint(0, 10),
            "funny": random.randint(0, 5),
            "cool": random.randint(0, 5),
        })
    return reviews


def write_jsonl(records: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    print(f"  Wrote {len(records):,} records → {path}")


def main(n_businesses: int, n_reviews: int, n_users: int) -> None:
    output_dir = Path(__file__).parent
    print(f"Generating sample data in {output_dir} ...")

    all_businesses: list[dict] = []
    per_city = max(1, n_businesses // len(CITIES))
    for city, state, lat, lng in CITIES:
        all_businesses.extend(generate_businesses(per_city, city, state, lat, lng))

    users = generate_users(n_users)
    reviews = generate_reviews(all_businesses, users, n_reviews)

    write_jsonl(all_businesses, output_dir / "yelp_academic_dataset_business.json")
    write_jsonl(users, output_dir / "yelp_academic_dataset_user.json")
    write_jsonl(reviews, output_dir / "yelp_academic_dataset_review.json")
    print("Done. Use --input data/sample/ with any pipeline job.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic Yelp sample data")
    parser.add_argument("--businesses", type=int, default=300)
    parser.add_argument("--reviews", type=int, default=3000)
    parser.add_argument("--users", type=int, default=500)
    args = parser.parse_args()
    main(args.businesses, args.reviews, args.users)
