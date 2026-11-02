# dheeraj_qiddiya_dbt_databricks

dbt project for the Qiddiya Medallion data warehouse on Databricks:
`edw_dev.landing_zone` (sources) → `bronze` → `silver` (incremental) → `gold` (star schema).

- Profile: `dheeraj_qiddiya_dbt_databricks` (template: `tests/profiles_template.yml`)
- Package: `dbt_utils` (run `dbt deps` once)

```powershell
uv run dbt deps
uv run dbt debug
uv run dbt build
```

See the root [README.md](../README.md) for full setup, credentials and troubleshooting.
