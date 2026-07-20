# Astronom

A personal Airflow learning project, built with the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview), used as a hands-on sandbox for working through Airflow fundamentals and Astronomer's learning path.

## What's in here

- `dags/` — DAGs pulled from Astronomer's example templates, used to explore core Airflow concepts: the TaskFlow API, dynamic task mapping, Assets, and a small ETL pipeline.
- `include/` — supporting files DAGs read from or write to at runtime (data files, helper modules, local databases).
- `tests/` — DAG validity and quality checks, run with `astro dev pytest`.
- `requirements.txt` / `packages.txt` — Python and OS-level dependencies for the DAGs in this project.

## Running it

```
astro dev start
```

Airflow UI: [localhost:8080](http://localhost:8080) — default login `admin` / `admin`.

```
astro dev stop
```

## Testing

```
astro dev pytest
```

Runs everything under `tests/` — DAG import checks, tagging/retry conventions, and task-level behavior checks.
