from __future__ import annotations

import json
import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def _rand_dt(start: datetime, end: datetime) -> datetime:
    delta = end - start
    seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=seconds)


def generate_users(fake: Faker, n: int = 80) -> pd.DataFrame:
    countries = ["BA", "HR", "RS", "DE", "AT", "US", "UK", None]
    rows = []
    base_date = datetime(2025, 12, 15)

    for i in range(1, n + 1):
        user_id = i
        full_name = fake.name()
        email = fake.email() if random.random() > 0.10 else None  # some null emails
        country = random.choice(countries)  # some null countries
        signup_date = (base_date + timedelta(days=random.randint(0, 60))).date().isoformat()

        rows.append(
            {
                "user_id": user_id,
                "full_name": full_name,
                "email": email,
                "country": country,
                "signup_date": signup_date,
            }
        )

    df = pd.DataFrame(rows)

    # introduce duplicates (same user_id) for dedup practice
    dup_sample = df.sample(5, random_state=42).copy()
    dup_sample["full_name"] = dup_sample["full_name"].apply(lambda x: x + " Jr.")
    df = pd.concat([df, dup_sample], ignore_index=True)

    return df


def generate_products(fake: Faker, n: int = 30) -> pd.DataFrame:
    categories = ["supplements", "protein", "vitamins", "snacks", "fitness"]
    brands = ["DynastyLab", "ProFuel", "VitaCore", "FitBite", "PowerPeak"]

    rows = []
    for i in range(1, n + 1):
        rows.append(
            {
                "product_id": i,
                "category": random.choice(categories),
                "brand": random.choice(brands),
                "product_name": fake.catch_phrase(),
            }
        )

    df = pd.DataFrame(rows)

    # add a couple nulls
    if n >= 5:
        df.loc[0, "brand"] = None
        df.loc[1, "category"] = None

    return df


def generate_orders(fake: Faker, n_orders: int = 600, n_users: int = 80, n_products: int = 30) -> pd.DataFrame:
    statuses = ["PAID", "CANCELLED", "REFUNDED"]
    start = datetime(2026, 2, 1, 0, 0, 0)
    end = datetime(2026, 2, 9, 23, 59, 59)

    rows = []
    for i in range(1, n_orders + 1):
        order_id = i
        user_id = random.randint(1, n_users)
        product_id = random.randint(1, n_products)

        # intentionally insert bad quantities/prices sometimes
        quantity = random.choice([1, 1, 2, 3, 4, 0, -1]) if random.random() < 0.06 else random.randint(1, 5)
        unit_price = round(random.uniform(5, 120), 2)
        if random.random() < 0.03:
            unit_price = -unit_price  # bad price

        order_ts = _rand_dt(start, end)
        status = random.choices(statuses, weights=[0.84, 0.10, 0.06])[0]

        # sometimes nulls
        if random.random() < 0.02:
            product_id = None
        if random.random() < 0.01:
            user_id = None

        rows.append(
            {
                "order_id": order_id,
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "order_ts": order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status,
                "event_date": order_ts.date().isoformat(),
            }
        )

    df = pd.DataFrame(rows)

    # introduce duplicate order_ids
    dup = df.sample(8, random_state=7).copy()
    df = pd.concat([df, dup], ignore_index=True)

    # introduce a few malformed timestamps
    if len(df) >= 3:
        df.loc[0, "order_ts"] = "not-a-timestamp"

    return df


def generate_events_jsonl(fake: Faker, n_events: int = 400, n_users: int = 80) -> list[dict]:
    event_types = ["page_view", "add_to_cart", "checkout_start", "purchase", "app_open"]
    devices = ["iPhone", "Android", "Web"]
    os_list = ["iOS", "Android", "Windows", "macOS", "Linux"]

    start = datetime(2026, 2, 1, 0, 0, 0)
    end = datetime(2026, 2, 9, 23, 59, 59)

    out = []
    for i in range(1, n_events + 1):
        ts = _rand_dt(start, end)
        out.append(
            {
                "event_id": i,
                "user_id": random.randint(1, n_users),
                "event_type": random.choice(event_types),
                "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "metadata": {
                    "device": random.choice(devices),
                    "os": random.choice(os_list),
                    "app_version": f"{random.randint(1,4)}.{random.randint(0,9)}.{random.randint(0,9)}",
                    "ip": fake.ipv4_public(),
                },
            }
        )

    # a few late events (older ts placed at end conceptually)
    for _ in range(5):
        ts = datetime(2026, 2, 2, 12, 0, 0)  # older
        out.append(
            {
                "event_id": n_events + random.randint(1000, 2000),
                "user_id": random.randint(1, n_users),
                "event_type": "purchase",
                "event_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                "metadata": {"device": "Web", "os": "Windows", "app_version": "2.1.0", "ip": fake.ipv4_public()},
            }
        )

    return out


def main() -> None:
    random.seed(123)
    fake = Faker()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    users = generate_users(fake)
    products = generate_products(fake)
    orders = generate_orders(fake, n_users=80, n_products=30)
    events = generate_events_jsonl(fake)

    users_path = RAW_DIR / "users.csv"
    products_path = RAW_DIR / "products.csv"
    orders_path = RAW_DIR / "orders.csv"
    events_path = RAW_DIR / "events.jsonl"

    users.to_csv(users_path, index=False)
    products.to_csv(products_path, index=False)
    orders.to_csv(orders_path, index=False)

    with events_path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    print("✅ Generated datasets:")
    print(f"- {users_path}")
    print(f"- {products_path}")
    print(f"- {orders_path}")
    print(f"- {events_path}")


if __name__ == "__main__":
    main()
