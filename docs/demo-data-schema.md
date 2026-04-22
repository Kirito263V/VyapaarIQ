# VyapaarIQ Demo Data Schema

This document describes the database structure you can share with GPT, Claude, or another generator to create a demo Excel workbook for `VyapaarIQ`.

The goal is to generate demo data for all business tables while excluding these authentication/profile tables:

- `users`
- `otp_verification`
- `business_profiles`

## Important Scope

Generate demo data only for these tables:

- `categories`
- `customers`
- `expenses`
- `products`
- `purchase_items`
- `purchases`
- `sale_items`
- `sales`
- `stock_alerts`
- `suppliers`

Do not generate or modify rows for:

- `users`
- `otp_verification`
- `business_profiles`

## Import-Friendly Rules

These rules matter if the generated Excel will be imported through the app's import screen instead of being inserted directly into SQLite.

- Do not include `id`, `user_id`, `created_at`, or any auto-generated primary key columns.
- Use sheet names exactly matching the dataset names above.
- Dates should use `YYYY-MM-DD`.
- Numeric fields should contain plain numbers only.
- `user_id` is added automatically by the app during import, so leave it out.
- Some foreign keys can be supplied as names and will be resolved by the app:
  - `products.category_id` can use category name
  - `products.supplier_id` can use supplier name
  - `sales.customer_id` can use customer name
  - `purchases.supplier_id` can use supplier name
  - `stock_alerts.product_id` can use product name
- `sale_items.sale_id` and `purchase_items.purchase_id` can now be resolved in two ways:
  - Directly by numeric `sale_id` or `purchase_id`
  - Indirectly by transaction context during import
- For `sale_items`, if `sale_id` is missing or non-numeric, the importer can resolve it from:
  - `customer_name`
  - `sale_date`
- For `purchase_items`, if `purchase_id` is missing or non-numeric, the importer can resolve it from:
  - `supplier_name`
  - `purchase_date`
- The contextual lookup only works when the matching parent transaction already exists in the database for that user.

## Best Practical Recommendation

For a clean first-pass demo import, generate these sheets first:

- `categories`
- `suppliers`
- `customers`
- `products`
- `expenses`
- `sales`
- `purchases`
- `stock_alerts`

Then handle these in a second pass only after the parent transactions already exist:

- `sale_items`
- `purchase_items`

If the goal is not app import but direct database seeding, then all ten tables can be generated together.

## Table Structure

### `categories`

Purpose: product grouping.

Columns for import:

- `name` - text, required, unique enough for lookup use
- `description` - text, optional

Notes:

- Referenced by `products.category_id`
- Best to keep category names unique

Suggested demo count: `8-15`

Example values:

- Grocery
- Beverages
- Snacks
- Personal Care
- Home Essentials

### `suppliers`

Purpose: vendor master data.

Columns for import:

- `name` - text, required
- `contact_person` - text, optional
- `phone` - text, optional
- `email` - text, optional
- `city` - text, optional
- `rating` - integer, optional, recommended range `1-5`

Notes:

- Referenced by `products.supplier_id`
- Referenced by `purchases.supplier_id`
- Supplier names should be unique enough for lookup matching

Suggested demo count: `8-20`

### `customers`

Purpose: customer master data.

Columns for import:

- `name` - text, required
- `phone` - text, optional
- `email` - text, optional
- `city` - text, optional
- `customer_type` - text, optional

Suggested `customer_type` values:

- `retail`
- `wholesale`
- `vip`
- `online`
- `walk-in`

Notes:

- Referenced by `sales.customer_id`
- Customer names should be unique enough for lookup matching

Suggested demo count: `30-80`

### `products`

Purpose: inventory master data.

Columns for import:

- `name` - text, required
- `category_id` - foreign key to `categories.id`, import can also use category name
- `supplier_id` - foreign key to `suppliers.id`, import can also use supplier name
- `sku` - text, should be unique across products
- `unit` - text
- `cost_price` - decimal number
- `selling_price` - decimal number
- `current_stock` - integer
- `reorder_level` - integer

Notes:

- `selling_price` should usually be greater than or equal to `cost_price`
- `current_stock` should be a non-negative integer
- `reorder_level` should be a non-negative integer, often lower than `current_stock`
- Referenced by `sale_items.product_id`
- Referenced by `purchase_items.product_id`
- Referenced by `stock_alerts.product_id`

Suggested `unit` values:

- `pcs`
- `box`
- `kg`
- `gm`
- `litre`
- `ml`
- `packet`
- `dozen`

Suggested demo count: `40-120`

### `sales`

Purpose: sales header or invoice-level transactions.

Columns for import:

- `customer_id` - foreign key to `customers.id`, import can also use customer name
- `sale_date` - date in `YYYY-MM-DD`
- `total_amount` - decimal number
- `payment_method` - text
- `notes` - text, optional

Suggested `payment_method` values:

- `cash`
- `upi`
- `card`
- `netbanking`
- `credit`

Notes:

- If sale line items also exist, then `total_amount` should equal the sum of related `sale_items.subtotal`
- Dates should be realistic and recent

Suggested demo count: `60-200`

### `sale_items`

Purpose: sale line items.

Columns for import:

- `sale_id` - foreign key to `sales.id`, optional if `customer_name` and `sale_date` are provided and match an existing sale
- `customer_name` - helper lookup field for import, used when `sale_id` is missing or non-numeric
- `sale_date` - helper lookup field for import, used when `sale_id` is missing or non-numeric
- `product_id` - foreign key to `products.id`, import can use product name in many flows, but numeric ID is safest
- `quantity` - integer
- `price` - decimal number
- `discount` - decimal number, usually `0-30`
- `subtotal` - decimal number

Formula:

- `subtotal = quantity * price * (1 - discount / 100)`

Notes:

- When using contextual lookup, the importer finds `sales.id` by matching `customer_name` and `sale_date`
- This sheet still depends on the parent `sales` rows already existing
- If generating data for direct DB seeding, ensure every `sale_item` belongs to an existing `sale_id`
- Every sale should have `1-5` line items

Suggested demo count: `150-600`

### `purchases`

Purpose: purchase order or stock intake headers.

Columns for import:

- `supplier_id` - foreign key to `suppliers.id`, import can also use supplier name
- `purchase_date` - date in `YYYY-MM-DD`
- `total_amount` - decimal number
- `status` - text

Suggested `status` values:

- `pending`
- `ordered`
- `received`
- `partial`
- `cancelled`

Notes:

- If purchase line items also exist, then `total_amount` should ideally equal the sum of `quantity * unit_cost` across related `purchase_items`

Suggested demo count: `40-120`

### `purchase_items`

Purpose: purchase line items.

Columns for import:

- `purchase_id` - foreign key to `purchases.id`, optional if `supplier_name` and `purchase_date` are provided and match an existing purchase
- `supplier_name` - helper lookup field for import, used when `purchase_id` is missing or non-numeric
- `purchase_date` - helper lookup field for import, used when `purchase_id` is missing or non-numeric
- `product_id` - foreign key to `products.id`, numeric ID preferred
- `quantity` - integer
- `unit_cost` - decimal number

Notes:

- When using contextual lookup, the importer finds `purchases.id` by matching `supplier_name` and `purchase_date`
- This sheet still depends on the parent `purchases` rows already existing
- Every purchase should have `1-5` line items

Suggested demo count: `100-400`

### `expenses`

Purpose: operating expense records.

Columns for import:

- `category` - text
- `amount` - decimal number
- `expense_date` - date in `YYYY-MM-DD`
- `description` - text

Suggested `category` values:

- `rent`
- `utilities`
- `salaries`
- `transport`
- `marketing`
- `maintenance`
- `packaging`
- `taxes`
- `insurance`
- `other`

Suggested demo count: `30-100`

### `stock_alerts`

Purpose: low stock or stock warning setup.

Columns for import:

- `product_id` - foreign key to `products.id`, import can also use product name
- `alert_type` - text
- `threshold` - integer
- `is_active` - integer, use `1` for active and `0` for inactive

Suggested `alert_type` values:

- `low_stock`
- `out_of_stock`

Notes:

- `threshold` should usually match or be close to `products.reorder_level`
- Recommended to create alerts only for a subset of products, not every product

Suggested demo count: `10-40`

## Relationships

Core relationships:

- `products.category_id -> categories.id`
- `products.supplier_id -> suppliers.id`
- `sales.customer_id -> customers.id`
- `sale_items.sale_id -> sales.id`
- `sale_items.product_id -> products.id`
- `purchases.supplier_id -> suppliers.id`
- `purchase_items.purchase_id -> purchases.id`
- `purchase_items.product_id -> products.id`
- `stock_alerts.product_id -> products.id`

## Data Consistency Rules

Use these rules when generating realistic demo data:

- Product names and SKUs should be unique.
- Customer and supplier names should be unique enough to support name-based lookups.
- `selling_price >= cost_price` for most products.
- `current_stock >= 0`.
- `reorder_level >= 0`.
- A minority of products should be low on stock.
- `stock_alerts.threshold` should usually be similar to product `reorder_level`.
- Sales and purchases should be spread across realistic dates, for example over the last 6 to 12 months.
- A few customers and suppliers can repeat across many transactions.
- Use Indian business-style names, cities, phone formats, and categories if you want the demo to feel natural for this app.

## Recommended Workbook Strategy

### Option A: Import-compatible workbook

Use these sheets:

- `categories`
- `suppliers`
- `customers`
- `products`
- `expenses`
- `sales`
- `purchases`
- `stock_alerts`

For this option:

- Use names in lookup fields where allowed
- You can include `sale_items` if the workbook is imported after matching `sales` rows already exist
- You can include `purchase_items` if the workbook is imported after matching `purchases` rows already exist

### Option B: Full relational seed workbook

Use all ten sheets if the data will be inserted directly into the database by a script instead of the app import UI.

For this option:

- Include consistent synthetic IDs in your design notes
- Ensure `sales.total_amount` matches related `sale_items.subtotal`
- Ensure `purchases.total_amount` matches related `purchase_items`

## Suggested Generation Prompt

You can give the following prompt to GPT or Claude:

```text
Generate a demo Excel workbook for the VyapaarIQ business app using the schema below.

Important:
- Do not generate data for users, otp_verification, or business_profiles.
- Generate realistic Indian small-business demo data.
- Use sheet names exactly as listed.
- Dates must be in YYYY-MM-DD format.
- Use plain numeric values for prices, quantities, totals, ratings, thresholds, and flags.
- Do not include id, user_id, or created_at columns.
- Keep names unique enough for lookup matching.
- Keep foreign-key relationships consistent.

Tables to generate:
1. categories(name, description)
2. suppliers(name, contact_person, phone, email, city, rating)
3. customers(name, phone, email, city, customer_type)
4. products(name, category_id or category name, supplier_id or supplier name, sku, unit, cost_price, selling_price, current_stock, reorder_level)
5. expenses(category, amount, expense_date, description)
6. sales(customer_id or customer name, sale_date, total_amount, payment_method, notes)
7. purchases(supplier_id or supplier name, purchase_date, total_amount, status)
8. stock_alerts(product_id or product name, alert_type, threshold, is_active)

If generating a direct database seed instead of an app-import workbook, also generate:
9. sale_items(sale_id or customer_name + sale_date, product_id, quantity, price, discount, subtotal)
10. purchase_items(purchase_id or supplier_name + purchase_date, product_id, quantity, unit_cost)

Allowed/reference values:
- customer_type: retail, wholesale, vip, online, walk-in
- payment_method: cash, upi, card, netbanking, credit
- purchase status: pending, ordered, received, partial, cancelled
- expense category: rent, utilities, salaries, transport, marketing, maintenance, packaging, taxes, insurance, other
- stock alert type: low_stock, out_of_stock
- is_active: 1 or 0

Consistency rules:
- selling_price should usually be >= cost_price
- current_stock and reorder_level must be non-negative integers
- a subset of products should have low stock
- totals should be realistic and consistent
- use Indian cities, people names, supplier names, and product naming patterns

Recommended volumes:
- categories: 10
- suppliers: 12
- customers: 50
- products: 80
- expenses: 60
- sales: 120
- purchases: 70
- stock_alerts: 20

Return the output as a workbook-style sheet plan or tabular sheet data that can be saved to Excel.
```

## Extra Note About Current App Behavior

The app import system can normalize alternate column names, but the safest output is to use the exact canonical column names listed in this document.
