# ci_cd/ — every CI/CD pipeline file lives here

Whichever tool you use, the deploy pipelines for this bundle go in this one folder.

```
ci_cd/
├── azure_devops/   deploy-dev.yml | deploy-uat.yml | deploy-prod.yml
└── github/         deploy-dev.yml | deploy-uat.yml | deploy-prod.yml
```

| Branch event | Target | What runs |
| --- | --- | --- |
| PR into `main` | dev | unit tests -> `bundle validate` (no deploy) |
| Merge into `main` | dev | unit tests -> validate -> deploy dev -> `bundle run unit_tests` -> `bundle run sales_integration_tests` |
| Push/merge into `uat` | uat | unit tests -> validate -> deploy uat -> both test jobs |
| Push/merge into `prod` | prod | validate -> deploy prod (behind the prod environment approval) |

Each file passes only its own environment's login (service connection or OIDC client ID) and
the target name; all per-environment values come from `resources/variables/variables.yml`.

## Azure DevOps
Point each pipeline at its file: New pipeline > Existing Azure Pipelines YAML file >
`/ci_cd/azure_devops/deploy-<env>.yml`. Azure DevOps runs YAML from any path. See `docs/setup-azure-devops.md`.

## GitHub Actions
GitHub only runs workflows from `.github/workflows/` at the repo root. Keep the source of truth here
and copy the three files from `ci_cd/github/` into `.github/workflows/`. See `docs/setup-github.md`.
