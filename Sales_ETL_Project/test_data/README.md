# Static test data (dev + uat only)

Small, known input so integration tests can check exact results. Copied into the dev/uat
landing volume by the `seed_test_data` task of the `sales_integration_tests` job (only missing files).

| File | Rows | Built-in cases | After the pipeline |
| --- | --- | --- | --- |
| orders/orders_001.json | 6 | order 2 has two versions (dedupe keeps the latest); order 3 has padded text (trim/upper); order 5 has a NULL customer_id (dropped by the silver expectation) | `silver_orders` = 4 rows → `expected_order_count: "4"` |
| customers/customers_001.json | 3 | C1–C3; every order's customer exists, so the orphan checks pass | `silver_customers` = 3 rows |

Change the files → update `expected_order_count` in the dev and uat sections of `resources/variables/variables.yml`.
Never load these into prod.
