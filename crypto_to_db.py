import requests
import psycopg2
import csv

DB_NAME = "crypto_project"
DB_USER = "postgres"
DB_PASSWORD = "your_password"
DB_HOST = "localhost"
DB_PORT = "5432"

API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"


conn = None
cur = None

try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cur = conn.cursor()

    # очищаем таблицу перед новой загрузкой
    cur.execute("TRUNCATE TABLE crypto_market RESTART IDENTITY;")

    # получаем данные из API
    response = requests.get(API_URL, timeout=10)
    response.raise_for_status()
    data = response.json()

    coins = data[:10]
    rows_for_csv = []

    for coin in coins:
        name = coin["name"]
        symbol = coin["symbol"]
        price = coin["current_price"]
        change = coin["price_change_percentage_24h"]
        market_cap = coin["market_cap"]
        volume = coin["total_volume"]

        cur.execute("""
            INSERT INTO crypto_market (
                name, symbol, price_usd, change_24h, market_cap, volume_24h
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (name, symbol, price, change, market_cap, volume))

        rows_for_csv.append([
            name,
            symbol,
            price,
            change,
            market_cap,
            volume
        ])

    conn.commit()

    with open("crypto_market.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "name",
            "symbol",
            "price_usd",
            "change_24h",
            "market_cap",
            "volume_24h"
        ])
        writer.writerows(rows_for_csv)

    print(f"{len(rows_for_csv)} coins saved to PostgreSQL successfully.")
    print("CSV exported: crypto_market.csv")

except Exception as e:
    print("Error:", e)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()