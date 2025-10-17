# todo
- [ ] composition over inheritance - decouple requests logic from ProviderClient classes 
- [ ] -> that will enable us to write unit tests, with feeding proper requests fixtures
- [ ] provide typing.Protocol for ProviderClients classes
  - [ ] maybe implement Aggregation service (combine multiple providers)<details><summary>Details</summary>
    ```python
    class OrdersAggregator:
        def __init__(self, providers: list[OrdersProvider]):
            self.providers = providers

        def fetch_all(self, since=None, until=None) -> list[Order]:
            # Optionally parallelize with asyncio if providers are IO-bound.
            orders = []
            for p in self.providers:
                orders.extend(p.fetch_orders(since, until))
            return self._dedupe(orders)

        def _dedupe(self, orders: list[Order]) -> list[Order]:
            # Decide merge keys/precedence rules:
            # - If the two systems share a marketplace order number, use that.
            # - Otherwise, choose a composite key (buyer_email + created_at ± tolerance).
            by_key = {}
            for o in orders:
                k = (o.external_id,)  # or another deterministic key
                if k not in by_key:
                    by_key[k] = o
                else:
                    by_key[k] = self._resolve_conflict(by_key[k], o)
            return list(by_key.values())

        def _resolve_conflict(self, a: Order, b: Order) -> Order:
            # Conflict strategy examples:
            #  - Prioritize a specific provider
            #  - Prefer most recently updated (if you carry updated_at)
            #  - Merge fields when one is missing (e.g., pick non-null email)
            # Keep provenance using `raw` if needed.
            if a.provider == "baselinker":
                return a
            return b

    ```
  </details>
- [x] Looker Studio graphs
  <details><summary>Details</summary>
  Here are practical, *insightful* charts you can build in Looker Studio with your current tables. I’ve grouped them by theme and noted **Dimensions / Metrics** (and when a small SQL view helps).

    ---

    ## Sales & revenue

    1. **Daily revenue by marketplace (stacked area)**

    * Source: `order` (or a daily rollup view).
    * **Dimension:** `DATE(created_at)` (or your `day_pl`).
    * **Breakdown:** `marketplace_name`.
    * **Metric:** `SUM(total_gross_pln)`.

    1. **Units sold vs. revenue (combo line)**

    * Join `order_item` → `order` → `marketplace`.
    * **Dimension:** `DATE(o.created_at)`.
    * **Metrics:** `SUM(oi.quantity)` and `SUM(oi.quantity * oi.price_pln)`.

    1. **Top SKUs by revenue (bar)**

    * Join `order_item` → `product`.
    * **Dimension:** `sku` (or `sku — product_name` concatenated).
    * **Metric:** `SUM(oi.quantity * oi.price_pln)`.

    1. **Pareto / ABC analysis (cumulative revenue share)**

    * Create a *custom query* that ranks SKUs by revenue and computes cumulative %.
    * **Dimension:** `rank_bucket` (A/B/C).
    * **Metric:** `SUM(revenue_pln)`.

    ---

    ## Pricing insights (from orders)

    1. **Daily average price (weighted) by SKU & marketplace (line)**

    * Use the *daily* custom query we wrote (`avg_price_pln_weighted`).
    * **Dimension:** `day_pl`.
    * **Breakdown:** `sku` or `marketplace_name`.
    * **Metric:** `avg_price_pln_weighted`.

    1. **Price dispersion per month (bar)**

    * Small SQL: `COUNT(DISTINCT oi.price_pln)` per `sku, marketplace, month`.
    * **Dimension:** `sku` (filter to one), **Breakdown:** `month`.
    * **Metric:** `distinct_prices`.

    1. **Price vs. units (scatter)**

    * Use a daily dataset grouped by `sku, marketplace, day`.
    * **X:** `avg_price_pln_weighted`.
    * **Y:** `units_sold`.
    * **Breakdown color:** `marketplace_name` (spot rough price‑demand patterns).

    ---

    ## Geography & shipping

    1. **Orders by country (choropleth map)**

    * Source: `order`.
    * **Geo dimension:** `country` (Type: Country).
    * **Metric:** `COUNT(*)` or `SUM(total_gross_pln)`.

    1. **Orders by city (bubble map)**

    * **Geo dimension:** `CONCAT(city, ', ', country)` (Type: City/Place).
    * **Metric:** `SUM(total_gross_pln)`.

    1.  **Delivery cost share (box/violin proxy via histogram)**

    * Calculated field: `delivery_share := delivery_cost_pln / NULLIF(total_gross_pln,0)`.
    * **Dimension:** `delivery_share` (bin/histogram).
    * **Metric:** `Record count` (or `COUNT(*)`).

    ---

    ## Catalog & marketplace health

    1.  **SKU × marketplace coverage (table)**

    * Start from `product_marketplace` and left‑join latest paid price (our “latest snapshot” query).
    * **Dimensions:** `sku`, `marketplace_name`.
    * **Metrics:** `latest_price_pln` (and a boolean “has\_price\_today”).

    1.  **Stock-outs vs sales (two lines or table)**

    * Use `stock_history` + a daily sales rollup.
    * **Dimension:** `day_pl`.
    * **Metrics:** `SUM(stock)` and `SUM(units_sold)`.
    * Add a filter for `sku`.

    ---

    ## Small SQL helpers you’ll likely reuse

    * **Daily sales rollup (per sku × marketplace)** – gives `units_sold`, `revenue_pln`, and weighted avg price:

    ```sql
    SELECT
        (o.created_at AT TIME ZONE 'Europe/Warsaw')::date AS day_pl,
        p.sku::text AS sku,
        p.name::text AS product_name,
        m.name::text AS marketplace_name,
        SUM(oi.quantity) AS units_sold,
        SUM(oi.quantity * oi.price_pln) AS revenue_pln,
        ROUND(SUM(oi.price_pln * oi.quantity) / NULLIF(SUM(oi.quantity),0), 2)
        AS avg_price_pln_weighted
    FROM order_item oi
    JOIN "order" o ON o.id = oi.order_id
    JOIN product p ON p.id = oi.product_id
    JOIN marketplace m ON m.id = o.marketplace_id
    GROUP BY 1,2,3,4;
    ```

    Connect this as a **Custom Query** and you can build charts 1, 2, 5, 7 directly.

    * **Monthly price dispersion (distinct daily prices)**

    ```sql
    SELECT
        DATE_TRUNC('month', o.created_at AT TIME ZONE 'Europe/Warsaw')::date AS month_pl,
        p.sku::text AS sku,
        m.name::text AS marketplace_name,
        COUNT(DISTINCT ROUND(oi.price_pln, 2)) AS distinct_prices
    FROM order_item oi
    JOIN "order" o ON o.id = oi.order_id
    JOIN product p ON p.id = oi.product_id
    JOIN marketplace m ON m.id = o.marketplace_id
    GROUP BY 1,2,3;
    ```

    ---

    ### Tips for Looker Studio setup

    * **Pre‑aggregate in SQL** for anything that needs window functions, deduping, or daily bucketing. It keeps charts fast and avoids “Record Count” confusion.
    * Cast text fields (`sku`, names) with `::text` so they aren’t treated as numbers.
    * Set report **Time Zone** to *Europe/Warsaw* and use `day_pl` consistently.
    * Prefer **weighted** averages for price (use quantity), and use **SUM(quantity \* price\_pln)** for revenue.

    If you tell me which 2–3 of these you want first, I can give you the exact final SQL tailored to those charts.
    </details>

- [x] add interface for loading data from .xml archive files
- [x] parallelize via chunks inserting orders<details><summary>Details</summary>

    Absolutely! Instead of creating a Prefect task for each order, you can create a task that processes a **batch of orders** at once. This way, you reduce task overhead and still benefit from parallelism by mapping over batches.

    Here's how you can do it:

    ---

    ### 1. Create a batch-processing task

    ```python
    from prefect import task

    @task
    def create_orders_batch(order_domain_dicts: list[dict]):
        from src.db.crud import get_or_create_order_efficient
        from src.db.engine import engine
        from sqlmodel import Session
        from src.domain.entities import Order as OrderDomain

        with Session(engine) as session:
            for order_dict in order_domain_dicts:
                order_domain = OrderDomain.model_validate(order_dict)
                get_or_create_order_efficient(session=session, order_domain=order_domain)
            session.commit()
    ```

    ---

    ### 2. Split your orders into batches

    ```python
    def chunked(lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]
    ```

    ---

    ### 3. Use Prefect mapping over batches

    ```python
    @flow
    def get_orders(...):
        # ...fetch orders as before...
        order_dicts = [o.model_dump(mode="json") for o in orders]
        batch_size = 100  # Tune this for your DB and infra
        batches = list(chunked(order_dicts, batch_size))
        create_orders_batch.map(batches)
    ```

    ---

    **Summary:**  
    - Each Prefect task now processes a batch of orders, reducing overhead and DB connection churn.
    - You still get parallelism, but with much less task overhead.
    - Tune `batch_size` for your workload and DB.

    This is a common and effective pattern for scalable, parallel ETL with Prefect and databases!
    https://chatgpt.com/c/68860a35-372c-8327-b648-0affda6e137a
    </details>

- [x] create streamlit app for stock tracking
https://chatgpt.com/c/68878a65-a4c8-832a-82fe-f34268c4a0e1

- [x] refactor flows.py to import vars only per flow when needed
- [x] add support for offers
- [x] add urls to images in Products, extract it from wharehouse products clients info
- [x] Prefect Workflow order: 
  - [x] 1. Pull and create / update data about Products - take precedence over the first source (Apilo, for Polish signs)
  - [x] 2. Pull and create / update data about Marketplaces
  - [x] 3. Pull and create Orders
  - [ ] 4. When to create Product-Marketplace relation:
     1. History data will be queried using:
     ```SQL
        SELECT DISTINCT
            p.id AS product_id,
            p.sku,
            p.name AS product_name,
            m.id AS marketplace_id,
            m.name AS marketplace_name
        FROM
            order_item oi
        JOIN
            product p ON oi.product_id = p.id
        JOIN
            "order" o ON oi.order_id = o.id
        JOIN
            marketplace m ON o.marketplace_id = m.id
        ORDER BY
            p.id, m.id;
     ```
     2. Custom flow for live data - adding and removing based on: 
        1. Add when iterating through baselinker products somehow? -> live data
        2. Add when adding Apilo Offers -> live data
- [x] \<feature> PriceHistory -> go through every offer of a product, and fill ProductHistory of only active offers, daily


