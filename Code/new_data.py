import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta


# Load existing dimension files
df_products         = pd.read_csv("products.csv")
df_customers        = pd.read_csv("customers.csv")
df_shippers         = pd.read_csv("shippers.csv")
df_statuses         = pd.read_csv("order_statuses.csv")
df_addresses        = pd.read_csv("addresses.csv")

# Load existing raw files to determine max keys
df_existing_orders  = pd.read_csv("orders.csv")
df_existing_items   = pd.read_csv("order_items.csv")
df_existing_payments= pd.read_csv("payments.csv")

max_order_id   = df_existing_orders["order_id"].max()
max_payment_id = df_existing_payments["payment_id"].max()

NUM_NEW_ORDERS = 3500       # Number of new orders to generate

# 1) Generate new orders
new_orders = []
for i in range(NUM_NEW_ORDERS):
    oid = int(max_order_id + i + 1)
    cid = int(df_customers.sample(1)["customer_id"])  # random customer
    sid = int(df_statuses.sample(1)["order_status_id"])  # random status
    shid = int(df_shippers.sample(1)["shipper_id"])
    # choose any address id for the customer
    aid = np.random.choice(df_addresses["address_id"], 1)[0]
    # random order date within last 2 days
    od = (datetime.now().date() - timedelta(days=random.randint(0,365)))
    new_orders.append({
        "order_id": oid,
        "customer_id": cid,
        "order_status_id": sid,
        "ship_address_id": aid,
        "shipper_id": shid,
        "order_date": od,
        "total_amount": 0  # placeholder
    })

df_new_orders = pd.DataFrame(new_orders)

# 2) Generate order_items and compute totals
new_items = []
for idx, row in df_new_orders.iterrows():
    oid = row["order_id"]
    n_items = random.randint(1,5)
    total = 0.0
    for _ in range(n_items):
        prod = df_products.sample(1).iloc[0]
        pid = int(prod["product_id"])
        price = float(prod.get("base_price", prod.get("unit_price", 0)))
        qty = random.randint(1,10)
        new_items.append({
            "order_id": oid,
            "product_id": pid,
            "quantity": qty,
            "unit_price": price
        })
        total += qty * price
    df_new_orders.at[idx, "total_amount"] = total

df_new_items = pd.DataFrame(new_items)

# 3) Generate payments for each new order
new_payments = []
for i, row in df_new_orders.iterrows():
    pid = int(max_payment_id + i + 1)
    oid = row["order_id"]
    pmeth = random.choice(["Visa", "Mastercard", "Stripe", "PayPal"])
    pdate = row["order_date"] + timedelta(days=random.randint(0,1))
    amt = row["total_amount"]
    stat = "Captured"
    new_payments.append({
        "payment_id": pid,
        "order_id": oid,
        "payment_method": pmeth,
        "payment_date": pdate,
        "amount": amt,
        "status": stat
    })

df_new_payments = pd.DataFrame(new_payments)

# 4) Write out incremental CSVs

df_new_orders.to_csv("new/orders.csv", index=False)
df_new_items.to_csv("new/order_items.csv", index=False)
df_new_payments.to_csv("new/payments.csv", index=False)

print("Generated incremental CSVs:")
print(f" - {len(df_new_orders)} orders")
print(f" - {len(df_new_items)} order_items")
print(f" - {len(df_new_payments)} payments")
