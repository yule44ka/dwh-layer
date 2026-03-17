from datetime import date
from dateutil.relativedelta import relativedelta

import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}


def create_tables(cur):
    cur.execute("""
        DROP TABLE IF EXISTS raw_transactions;
        DROP TABLE IF EXISTS raw_subscriptions;
        DROP TABLE IF EXISTS raw_customers;

        CREATE TABLE raw_customers (
            customer_id INT PRIMARY KEY,
            company_name TEXT NOT NULL,
            country TEXT NOT NULL,
            signup_date DATE NOT NULL
        );

        CREATE TABLE raw_subscriptions (
            sub_id INT PRIMARY KEY,
            customer_id INT NOT NULL,
            plan_type TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE,
            amount NUMERIC(10, 2) NOT NULL,
            CONSTRAINT fk_customer
                FOREIGN KEY (customer_id)
                REFERENCES raw_customers(customer_id)
        );

        CREATE TABLE raw_transactions (
            tx_id INT NOT NULL,
            sub_id INT NOT NULL,
            tx_date DATE NOT NULL,
            status TEXT NOT NULL,
            CONSTRAINT fk_subscription
                FOREIGN KEY (sub_id)
                REFERENCES raw_subscriptions(sub_id)
        );
    """)


def insert_test_data(cur):
    customers = [
        (1, "Acme Corp", "USA", "2023-01-08"),
        (2, "TechSoft GmbH", "Germany", "2023-02-15"),
        (3, "DataWorks Ltd", "UK", "2023-02-28"),
    ]

    # monthly - 120 euro/month, annual - 100 euro/month
    subscriptions = [
        (101, 1, "Monthly", "2023-01-10", "2023-04-10", 360.00),
        (102, 2, "Annual", "2023-02-15", "2024-02-15", 1200.00),
        (103, 3, "Monthly", "2023-03-01", None, 4440.00), # until March 2026
    ]

    transactions = [
        (1, 101, "2023-01-10", "Failed"),
        (2, 101, "2023-01-10", "Success"),
        (3, 101, "2023-01-10", "Success"), # accidental payment
        (4, 101, "2023-01-10", "Refunded"), # refund of second payment
        (5, 101, "2023-02-10", "Success"),
        (6, 101, "2023-03-10", "Success"),
        (7, 101, "2023-04-10", "Failed"),
        (8, 102, "2023-02-15", "Success"),
        (8, 102, "2023-02-15", "Success"),  # duplicate
    ]

    # 103 sub
    start = date(2023, 3, 1)
    end = date(2026, 3, 1)

    tx_id = 9
    current = start

    while current <= end:
        transactions.append(
            (tx_id, 103, current.strftime("%Y-%m-%d"), "Success")
        )
        tx_id += 1
        current += relativedelta(months=1)

    cur.executemany("""
        INSERT INTO raw_customers (customer_id, company_name, country, signup_date)
        VALUES (%s, %s, %s, %s)
    """, customers)

    cur.executemany("""
        INSERT INTO raw_subscriptions (sub_id, customer_id, plan_type, start_date, end_date, amount)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, subscriptions)

    cur.executemany("""
        INSERT INTO raw_transactions (tx_id, sub_id, tx_date, status)
        VALUES (%s, %s, %s, %s)
    """, transactions)


def print_counts(cur):
    for table_name in ["raw_customers", "raw_subscriptions", "raw_transactions"]:
        cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)))
        count = cur.fetchone()[0]
        print(f"{table_name}: {count} rows")


def main():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False

        with conn.cursor() as cur:
            create_tables(cur)
            insert_test_data(cur)
            print_counts(cur)

        conn.commit()
        print("Tables created and test data inserted successfully.")

    except Exception as e:
        if conn is not None:
            conn.rollback()
        print("Error:", e)

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()