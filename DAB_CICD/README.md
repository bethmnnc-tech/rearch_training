# sales_etl — Declarative Automation Bundle with tests and CI/CD

One bundle, three targets (dev, uat, prod). The same jobs and pipeline deploy everywhere;
only the per-target variables change. Tests run in CI before and after every dev/uat deploy.
Companion cheat sheets: "Declarative Automation Bundles for CI/CD" and "PySpark Unit Testing + CI/CD".

## Layout

```
DAB CICD Template/
├── databricks.yml                     # bundle root: include + dev/uat/prod targets
├── pyproject.toml                     # pytest config (testpaths, pythonpath=src, markers)
├── requirements-dev.txt               # pyspark (pin to runtime), pytest, pytest-cov
├── resources/
│   ├── variables/                     # variables.yml + dev/uat/prod.yml + integration_test_variables.yml
│   ├── jobs/sales_daily.job.yml       # the real job (all targets)
│   ├── pipelines/sales_pipeline.pipeline.yml
│   └── test_jobs/test_jobs.yml        # unit_tests + sales_integration_tests (dev + uat ONLY)
├── src/
│   ├── sales_etl/                     # custom functions: transforms, data_quality, integration_checks
│   ├── pipelines/sales/transformations/
│   │   ├── bronze_orders.sql          # shared pipeline code
│   │   ├── silver_gold.py             # shared pipeline code (uses sales_etl)
│   │   └── integration_tests.py       # Method 1 test views (only when run_integration_tests = true)
│   ├── notebooks/
│   │   ├── publish_gold.py
│   │   ├── run_tests.py               # runs pytest on Databricks
│   │   └── integration_tests/         # Method 2 check notebooks
│   └── sql/dq_checks.sql
├── tests/                             # pytest: levels 1–9 (+ conftest.py)
├── tests_sdp_editor/                  # SDP TestPipeline (Beta) – Pipelines Editor only, not CI
└── cicd/
    ├── github/deploy-dev.yml | deploy-uat.yml | deploy-prod.yml
    └── azure-devops/deploy-dev.yml | deploy-uat.yml | deploy-prod.yml
```

## What runs when

| Trigger | Steps |
| --- | --- |
| Pull request into develop/main | unit tests on the runner → `bundle validate` (no deploy) |
| Push to develop | unit tests → validate → deploy dev → `bundle run unit_tests` → `bundle run sales_integration_tests` |
| Push to main | unit tests → validate → deploy uat → `bundle run unit_tests` → `bundle run sales_integration_tests` |
| Manual run on main + approval | validate → deploy prod (the commit that already passed uat; no test jobs exist in prod) |

- **Unit tests** (before deploy): `pytest -m "not integration"` on the CI runner with local Spark. No workspace, no secrets.
- **`unit_tests` job** (after deploy): pytest on Databricks, including the `@integration` test that writes Delta tables to a temporary schema.
- **`sales_integration_tests` job** (after deploy): unit tests → pipeline refresh (Method 1 SDP expectation test views run here in dev/uat) → Method 2 check notebooks → publish gold.

## Run locally

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt                   # Java 17+ on PATH
pytest -m "not integration"                           # what CI runs before deploy
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run unit_tests -t dev
databricks bundle run sales_integration_tests -t dev
```

## Before first use

1. Replace the workspace hosts in `databricks.yml` and the service principal IDs in `resources/variables/uat.yml` / `prod.yml`.
2. Load static test files (orders + customers JSON) into the dev and uat landing volumes; set `expected_order_count` in `resources/variables/integration_test_variables.yml` to match.
3. Give the CI service principals `USE CATALOG` + `CREATE SCHEMA` on the dev/uat catalogs (temp test schemas).
4. GitHub: copy `cicd/github/*.yml` into `.github/workflows/`. Azure DevOps: register each `cicd/azure-devops/*.yml` as a pipeline.
5. Make the unit-test check required on `develop` and `main` (branch protection / branch policy).
