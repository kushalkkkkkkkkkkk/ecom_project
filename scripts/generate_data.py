from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

import pandas as pd
from faker import Faker


ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

fake = Faker()
Faker.seed(2024)
random.seed(2024)


def _random_count() -> int:
    """Return a random row count between 100 and 200."""
    return random.randint(100, 200)


@dataclass
class Product:
    product_id: int
    price: float


def generate_customers(num_rows: int) -> pd.DataFrame:
    customers: List[dict] = []
    for customer_id in range(1, num_rows + 1):
        created_at = fake.date_time_between(start_date="-2y", end_date="now")
        customers.append(
            {
                "customer_id": customer_id,
                "name": fake.name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "city": fake.city(),
                "created_at": created_at.isoformat(),
            }
        )
    fake.unique.clear()
    return pd.DataFrame(customers)


def generate_products(num_rows: int) -> pd.DataFrame:
    categories = [
        "Electronics",
        "Home",
        "Fashion",
        "Beauty",
        "Outdoors",
        "Toys",
        "Books",
    ]
    products: List[dict] = []
    for product_id in range(1, num_rows + 1):
        price = round(random.uniform(5, 500), 2)
        products.append(
            {
                "product_id": product_id,
                "product_name": fake.catch_phrase(),
                "category": random.choice(categories),
                "price": price,
            }
        )
    return pd.DataFrame(products)


def generate_orders(num_rows: int, customer_ids: List[int]) -> pd.DataFrame:
    orders: List[dict] = []
    for order_id in range(1, num_rows + 1):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "order_date": order_date.isoformat(),
                "total_amount": 0.0,
            }
        )
    return pd.DataFrame(orders)


def generate_order_items(
    num_rows: int, order_ids: List[int], products: Dict[int, Product]
) -> pd.DataFrame:
    order_items: List[dict] = []
    product_ids = list(products.keys())
    item_id = 1

    # Ensure every order has at least one item
    for order_id in order_ids:
        if item_id > num_rows:
            break
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        base_price = products[product_id].price
        item_price = round(base_price * random.uniform(0.9, 1.1), 2)
        order_items.append(
            {
                "item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "item_price": item_price,
            }
        )
        item_id += 1

    # Fill remaining rows randomly
    while item_id <= num_rows:
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        base_price = products[product_id].price
        item_price = round(base_price * random.uniform(0.9, 1.1), 2)
        order_items.append(
            {
                "item_id": item_id,
                "order_id": order_id,
                "product_id": product_id,
                "quantity": quantity,
                "item_price": item_price,
            }
        )
        item_id += 1

    return pd.DataFrame(order_items)


def generate_payments(
    num_rows: int, order_ids: List[int], order_dates: Dict[int, datetime]
) -> pd.DataFrame:
    payment_methods = ["Credit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]
    payment_statuses = ["Completed", "Pending", "Failed"]

    payments: List[dict] = []
    for payment_id in range(1, num_rows + 1):
        order_id = random.choice(order_ids)
        payment_method = random.choice(payment_methods)
        payment_status = random.choices(
            payment_statuses, weights=[0.7, 0.2, 0.1], k=1
        )[0]

        order_date = order_dates[order_id]
        payment_date = order_date + timedelta(
            days=random.randint(0, 5), hours=random.randint(0, 23)
        )

        payments.append(
            {
                "payment_id": payment_id,
                "order_id": order_id,
                "payment_method": payment_method,
                "payment_status": payment_status,
                "payment_date": payment_date.isoformat(),
            }
        )
    return pd.DataFrame(payments)


def main() -> None:
    DATA_DIR.mkdir(exist_ok=True, parents=True)

    num_customers = _random_count()
    num_products = _random_count()
    num_orders = _random_count()
    num_order_items = max(num_orders, _random_count())
    num_payments = _random_count()

    customers_df = generate_customers(num_customers)
    products_df = generate_products(num_products)
    orders_df = generate_orders(num_orders, customers_df["customer_id"].tolist())

    product_lookup = {
        row.product_id: Product(product_id=row.product_id, price=row.price)
        for row in products_df.itertuples(index=False)
    }
    order_dates = {
        row.order_id: datetime.fromisoformat(row.order_date)
        for row in orders_df.itertuples(index=False)
    }

    order_items_df = generate_order_items(
        num_order_items, orders_df["order_id"].tolist(), product_lookup
    )

    # Update order totals based on generated items
    totals = (
        order_items_df.assign(line_total=lambda df: df["quantity"] * df["item_price"])
        .groupby("order_id")["line_total"]
        .sum()
    )
    orders_df = orders_df.set_index("order_id")
    orders_df.loc[totals.index, "total_amount"] = totals.round(2)
    orders_df["total_amount"] = orders_df["total_amount"].fillna(0.0).round(2)
    orders_df = orders_df.reset_index()

    payments_df = generate_payments(
        num_payments, orders_df["order_id"].tolist(), order_dates
    )

    outputs = {
        "customers.csv": customers_df,
        "products.csv": products_df,
        "orders.csv": orders_df,
        "order_items.csv": order_items_df,
        "payments.csv": payments_df,
    }

    for filename, df in outputs.items():
        output_path = DATA_DIR / filename
        df.to_csv(output_path, index=False)
        print(f"Wrote {len(df):>3} rows to {output_path.relative_to(ROOT_DIR)}")


if __name__ == "__main__":
    main()

