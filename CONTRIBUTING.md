# Contributing

Thanks for your interest in extending this demo. Here are a few guidelines to
keep things consistent.

## Adding a new notebook

1. Number it sequentially (e.g. `10_new_feature.ipynb`)
2. Place it under `src/notebooks/`
3. First code cell should set `CATALOG = spark.catalog.currentCatalog()` and
   any required schema variables
4. Include `%pip install` for any non-standard dependencies
5. Use `# COMMAND ----------` separators between logical sections
6. Add a corresponding task to the appropriate job in `databricks.yml`

## Style conventions

- Use f-strings for SQL queries that reference catalog/schema variables
- Print a `✓` confirmation after each major step completes
- Include timing estimates in markdown headers where helpful
- Keep cell outputs clean — `display()` for DataFrames, `print()` for status

## Testing changes

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run <job_name> -t dev
```

## Reporting issues

Open an issue in the repo with:
- Which notebook / cell failed
- The full error traceback
- Your compute type (serverless, classic, job cluster)
- Databricks Runtime version
