import os
import csv
import random
from datetime import datetime, timedelta

if not os.path.exists("data"):
    os.mkdir("data")

if not os.path.exists("data/shops.csv"):
  # Genera shops.csv
  cities = ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence", "Bari", "Catania"]
  shops = []
  for city in cities:
    for i in range(10):
      shops.append([city, f"Shop {i+1} in {city}"])

  with open('data/shops.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["City", "Shop Name"])
    writer.writerows(shops)

if not os.path.exists("data/sales.csv"):
  # Genera sales.csv
  products = ["Dress", "Car", "Videogame", "Laptop", "TV"]
  sales = []
  start_date = datetime(2023, 1, 1)
  end_date = datetime.now()

  for i in range(1000):
    product = random.choice(products)
    quantity = random.randint(1, 10)
    price = round(random.uniform(10, 100), 2)
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    timestamp = random_date.strftime("%Y-%m-%d %H:%M:%S")
    sales.append([product, quantity, price, timestamp])

  with open('data/sales.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Product", "Quantity", "Price", "Timestamp"])
    writer.writerows(sales)

