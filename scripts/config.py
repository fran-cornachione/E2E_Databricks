"""
config.py — Shared constants and ID pools
Both generate_data.py and stream_producer.py import from here
to guarantee consistent IDs across all tables.
"""

# ── Volume sizes ─────────────────────────────────────────────────────────────
N_CUSTOMERS = 2_000
N_PRODUCTS  = 500
N_ORDERS    = 15_000
N_EVENTS    = 20   # only used by generate_data.py for the static snapshot

# ── Shared ID pools ───────────────────────────────────────────────────────────
CUSTOMER_IDS = list(range(1, N_CUSTOMERS + 1))
PRODUCT_IDS  = list(range(1, N_PRODUCTS + 1))

# ── Domain constants ──────────────────────────────────────────────────────────
CHANNELS         = ["web", "mobile_ios", "mobile_android", "app"]
PAYMENT_METHODS  = ["credit_card", "debit_card", "paypal", "transfer", "cash_on_delivery"]
ORDER_STATUSES   = ["completed", "completed", "completed", "cancelled", "disputed", "refunded"]
DELIVERY_STATUSES = ["delivered", "delivered", "delivered", "in_transit", "failed", "returned"]

CATEGORIES = {
    "Electronics":   ["Smartphones", "Laptops", "Tablets", "Headphones", "Cameras"],
    "Clothing":      ["T-Shirts", "Jeans", "Dresses", "Shoes", "Accessories"],
    "Home & Garden": ["Furniture", "Kitchen", "Bedding", "Tools", "Decor"],
    "Sports":        ["Gym Equipment", "Outdoor", "Cycling", "Swimming", "Team Sports"],
    "Books":         ["Fiction", "Non-Fiction", "Technical", "Children", "Comics"],
}
FLAT_CATEGORIES = [(cat, sub) for cat, subs in CATEGORIES.items() for sub in subs]