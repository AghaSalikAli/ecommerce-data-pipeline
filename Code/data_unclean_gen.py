import random
from faker import Faker
import pandas as pd
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(0)
random.seed(0)

# Utility functions
def generate_unique_ids(n):
    ids = list(range(1, n + 1))
    random.shuffle(ids)
    return ids

N = 7000

# ID pools
customer_ids = generate_unique_ids(N)
seller_ids = generate_unique_ids(N)
category_ids = generate_unique_ids(N)
product_ids = generate_unique_ids(N)
order_ids = generate_unique_ids(N)
review_ids = generate_unique_ids(N)
payment_ids = generate_unique_ids(N)
address_ids = generate_unique_ids(N)
wishlist_ids = generate_unique_ids(N)
delivery_ids = generate_unique_ids(N)

# Storage
dataframes = {
    "customers": [], "sellers": [], "categories": [], "products": [],
    "orders": [], "order_items": [], "addresses": [],
    "reviews": [], "payments": [], "wishlists": [], "deliveries": []
}

# Helpers to introduce errors
def maybe_null(val):
    return val if random.random() > 0.1 else None

def maybe_wrong_type(val):
    return val if random.random() > 0.1 else random.choice(['N/A', 'unknown', '###'])

def maybe_invalid_enum(enum_list):
    return random.choice(enum_list + ['INVALID', ''])

def maybe_invalid_fk(fk_list):
    return random.choice(fk_list + [999999, None])

# Data generators
def generate_customer(cid):
    return {
        'Customer_id': cid,
        'FirstName': maybe_null(fake.first_name()),
        'MiddleName': '' if random.random() > 0.9 else fake.first_name(),
        'LastName': fake.last_name(),
        'Email': fake.email() if random.random() > 0.05 else 'not-an-email',
        'DateOfBirth': maybe_wrong_type(fake.date_of_birth(minimum_age=18, maximum_age=80)),
        'Phone': maybe_wrong_type(fake.random_number(digits=10)),
        'Age': maybe_null(random.randint(18, 80))
    }

def generate_seller(sid):
    return {
        'Seller_id': sid,
        'Name': maybe_null(fake.company()),
        'Phone': maybe_wrong_type(fake.phone_number()),
        'Total_sales': maybe_wrong_type(round(random.uniform(1000, 100000), 2))
    }

def generate_category(cid):
    return {
        'Category_id': cid,
        'Category_name': maybe_null(fake.word()),
        'Description': fake.sentence()
    }

def generate_product(pid):
    return {
        'Product_id': pid,
        'Product_name': maybe_wrong_type(fake.word()),
        'MRP': maybe_wrong_type(round(random.uniform(5, 2000), 2)),
        'Stock': random.choice([True, False, 'yes', 'no']) if random.random() < 0.1 else random.choice([True, False]),
        'Brand': fake.company(),
        'Category_CategoryID': maybe_invalid_fk(category_ids),
        'Seller_Seller_id': maybe_invalid_fk(seller_ids)
    }

def generate_order(oid):
    return {
        'Order_id': oid,
        'Order_date': maybe_wrong_type(fake.date_time_between(start_date='-1y', end_date='now')),
        'Order_amount': maybe_wrong_type(round(random.uniform(100, 3000), 2)),
        'Shipping_Date': maybe_wrong_type(fake.date_time_between(start_date='now', end_date='+10d')),
        'Order_status': maybe_invalid_enum(['Pending', 'Shipped', 'Delivered', 'Cancelled']),
        'Customer_customer_id': maybe_invalid_fk(customer_ids)
    }

def generate_orderitem():
    return {
        'Order_Order_id': maybe_invalid_fk(order_ids),
        'Product_product_id': maybe_invalid_fk(product_ids),
        'MRP': maybe_wrong_type(round(random.uniform(5, 2000), 2)),
        'Quantity': maybe_wrong_type(random.randint(1, 5))
    }

def generate_address(aid):
    return {
        'Address_id': aid,
        'Apart_no': maybe_wrong_type(random.randint(1, 300)),
        'ApartName': maybe_null(fake.street_name()),
        'StreetName': maybe_null(fake.street_name()),
        'State': fake.state(),
        'City': maybe_null(fake.city()),
        'Pincode': maybe_wrong_type(fake.random_number(digits=6)),
        'Customer_Customer_id': maybe_invalid_fk(customer_ids)
    }

def generate_review(rid):
    return {
        'Review_id': rid,
        'Description': fake.sentence(),
        'Rating': maybe_wrong_type(random.choice([1, 2, 3, 4, 5])),
        'Product_Product_id': maybe_invalid_fk(product_ids),
        'Customer_Customer_id': maybe_invalid_fk(customer_ids)
    }

def generate_payment(pid):
    return {
        'Payment_id': pid,
        'ORDER_Order_id': maybe_invalid_fk(order_ids),
        'PaymentMode': maybe_invalid_enum(['Card', 'Cash', 'UPI', 'Wallet']),
        'Customer_Customer_id': maybe_invalid_fk(customer_ids),
        'DateOfPayment': maybe_wrong_type(fake.date_time_between(start_date='-1y', end_date='now'))
    }

def generate_wishlist(wid):
    return {
        'Wishlist_id': wid,
        'Customer_id': maybe_invalid_fk(customer_ids),
        'Created_at': maybe_wrong_type(fake.date_time_this_year())
    }

def generate_delivery(did):
    return {
        'Delivery_id': did,
        'Order_id': maybe_invalid_fk(order_ids),
        'Delivery_status': maybe_invalid_enum(['Pending', 'Shipped', 'Delivered']),
        'Delivery_date': maybe_wrong_type(fake.date_time_between(start_date='now', end_date='+20d')),
        'Courier': maybe_null(fake.company())
    }

# Populate all tables
for i in range(N):
    dataframes['customers'].append(generate_customer(customer_ids[i]))
    dataframes['sellers'].append(generate_seller(seller_ids[i]))
    dataframes['categories'].append(generate_category(category_ids[i]))
    dataframes['products'].append(generate_product(product_ids[i]))
    dataframes['orders'].append(generate_order(order_ids[i]))
    dataframes['addresses'].append(generate_address(address_ids[i]))
    dataframes['reviews'].append(generate_review(review_ids[i]))
    dataframes['payments'].append(generate_payment(payment_ids[i]))
    dataframes['wishlists'].append(generate_wishlist(wishlist_ids[i]))
    dataframes['deliveries'].append(generate_delivery(delivery_ids[i]))

for _ in range(N):
    dataframes['order_items'].append(generate_orderitem())

# Save all to CSV
for table, data in dataframes.items():
    df = pd.DataFrame(data)
    df.to_csv(f"{table}.csv", index=False)