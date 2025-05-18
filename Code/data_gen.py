import pandas as pd, numpy as np, random
from faker import Faker
fake = Faker()

# --- A. PARENT TABLES --------------------------------------------------------
customers = pd.DataFrame({
    "customer_id": range(1, 5001),
    "first_name":  [fake.first_name() for _ in range(5000)],
    "last_name":   [fake.last_name()  for _ in range(5000)],
    "email":       [fake.unique.email() for _ in range(5000)],
    "phone":       [fake.phone_number() for _ in range(5000)],
    "date_joined": [fake.date_time_this_decade() for _ in range(5000)]
})
print('Customers generated')

shippers = pd.DataFrame({
    "shipper_id": range(1, 501),
    "name": [fake.company() for _ in range(500)],
    "phone": [fake.phone_number() for _ in range(500)],
    "tracking_url_template": [f"https://track.example/{i}/{{}}" for i in range(500)]
})
print('Shippers generated')

order_statuses = pd.DataFrame({
    "order_status_id": [1,2,3,4],
    "status_name": ["Pending","Processing","Shipped","Delivered"],
    "description": ["", "", "", ""]
})
print('Order statuses generated')


# # save to CSV
customers.to_csv("customers.csv", index=False)
shippers.to_csv("shippers.csv", index=False)
order_statuses.to_csv("order_statuses.csv", index=False)

categories = pd.read_csv("categories.csv")
print('Categories generated')

product_names = pd.read_csv("product_names.csv")

product_names = product_names.sample(n=20000, random_state=42)

# --- B. PRODUCTS -------------------------------------------------------------
products = pd.DataFrame({
    "product_id": range(1, 20001),
    "name": [product_names["Product Name"].iloc[i] for i in range(20000)],
    "description": [fake.text(max_nb_chars=60) for _ in range(20000)],
    "sku": [fake.unique.bothify(text="??#####") for _ in range(20000)],
    "base_price": [round(random.uniform(5,500),2) for _ in range(20000)],
    "created_at": [fake.date_time_this_decade() for _ in range(20000)]
})
products.to_csv("products.csv", index=False)
print('Products generated')

# --- C. DEPENDENT TABLES -----------------------------------------------------
inventory = pd.DataFrame({
    "inventory_id": range(1,20001),
    "product_id": products["product_id"],
    "quantity_available": np.random.randint(0,300,20000),
    "reorder_level": np.random.randint(20,50,20000),
    "last_updated": [fake.date_time_this_year() for _ in range(20000)]
})
inventory.to_csv("inventory.csv", index=False)
print('Inventory generated')

addresses = pd.DataFrame({
    "address_id": range(1,3001),
    "customer_id": np.random.choice(customers["customer_id"],3000),
    "line1": [fake.street_address() for _ in range(3000)],
    "line2": [fake.secondary_address() for _ in range(3000)],
    "city": [fake.city() for _ in range(3000)],
    "state": [fake.state_abbr() for _ in range(3000)],
    "postal_code": [fake.postcode() for _ in range(3000)],
    "country": ["USA"]*3000,
    "is_default": np.random.choice([True,False], 3000, p=[0.7,0.3]),
})
addresses.to_csv("addresses.csv", index=False)
print('Addresses generated')

# --- D. JUNCTIONS (ONE PARENT EACH) -----------------------------------------
product_categories = pd.DataFrame({
    "product_id": np.repeat(products["product_id"], 2),    # three cats per product
    "category_id": np.random.choice(categories["category_id"], 40000),
}).drop_duplicates()
product_categories.to_csv("product_categories.csv", index=False)
print('Product categories generated')

customer_wishlist = pd.DataFrame({
    "customer_id": np.random.choice(customers["customer_id"], 3000),
    "product_id": np.random.choice(products["product_id"], 3000),
    "added_at": [fake.date_time_this_year() for _ in range(3000)]
}).drop_duplicates()
customer_wishlist.to_csv("customer_wishlist.csv", index=False)
print('Customer wishlist generated')

# --- E. ORDERS ---------------------------------------------------------------
orders = pd.DataFrame({
    "order_id": range(1,10001),
    "customer_id": np.random.choice(customers["customer_id"], 10000),
    "order_status_id": np.random.choice(order_statuses["order_status_id"], 10000),
    "ship_address_id": np.random.choice(addresses["address_id"], 10000),
    "shipper_id": np.random.choice(shippers["shipper_id"], 10000),
    "order_date": [fake.date_time_this_decade() for _ in range(10000)],
    "total_amount": 0.0   # will update later
})
orders.to_csv("orders.csv", index=False)

# --- F. PAYMENTS -------------------------------------------------------------
payments = pd.DataFrame({
    "payment_id": range(1,10001),
    "order_id": orders["order_id"],
    "payment_method": np.random.choice(["Visa","Mastercard","Stripe","PayPal"], 10000),
    "payment_date": orders["order_date"],
    "amount": 0.0,   # same as order total later
    "status": "Captured"
})
payments.to_csv("payments.csv", index=False)

# --- G. ORDER_ITEMS & REVIEWS -----------------------------------------------
order_items = []
for oid in orders["order_id"]:
    n_lines = random.randint(1,4)
    chosen = np.random.choice(products["product_id"], n_lines, replace=False)
    for pid in chosen:
        qty  = random.randint(1,10)
        price = products.loc[products["product_id"]==pid, "base_price"].iloc[0]
        order_items.append([oid, pid, qty, price])

order_items = pd.DataFrame(order_items, columns=["order_id","product_id","quantity","unit_price"])
order_items.to_csv("order_items.csv", index=False)
print('Order items generated')

# backfill totals
totals = order_items.groupby("order_id").apply(lambda df: (df["quantity"]*df["unit_price"]).sum())
orders["total_amount"]  = orders["order_id"].map(totals)
payments["amount"]      = payments["order_id"].map(totals)
orders.to_csv("orders.csv", index=False)
payments.to_csv("payments.csv", index=False)
print('Totals backfilled in orders and payments')

# REVIEWS
reviews = pd.DataFrame({
    "review_id": range(1,3001),
    "product_id": np.random.choice(products["product_id"], 3000),
    "customer_id": np.random.choice(customers["customer_id"], 3000),
    "rating": np.random.randint(1,6,3000),
    "review_text": [fake.sentence() for _ in range(3000)],
    "review_date": [fake.date_time_this_year() for _ in range(3000)]
})
reviews.to_csv("reviews.csv", index=False)
print('Reviews generated')