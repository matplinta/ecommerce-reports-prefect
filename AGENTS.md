# Environment and deployment rationale

## Prefect Managed runtime

Production flows run in a Prefect Managed work pool. Its runtime is separate from
the developer's local environment and already contains Prefect before deployment
pull steps execute. The user has experienced runtime version changes over time;
do not assume local package versions match the managed container.

The current environment setup is intentional. Previous attempts to install a
fully pinned local environment into the managed runtime repeatedly failed after
Prefect version changes. The current approach worked for a long time by adapting
dependencies to the Prefect version present in the container.

Installing a different Prefect version during pull steps can mix already imported
code with newly installed modules. This failure pattern is documented in
[Prefect issue #18394](https://github.com/PrefectHQ/prefect/issues/18394).

## Why `prepare_req.sh` exists

`prefect.yaml` clones the repository's `main` branch and then runs
`bash prepare_req.sh` in the cloned directory. The script:

1. Installs `build-essential` to provide compilation tools for dependencies that
   need source builds in the Linux runtime.
2. Reads the Prefect version already installed in the image.
3. Writes a temporary constraint for that exact Prefect version.
4. Uses `uv pip compile pyproject.toml` with that constraint to resolve application
   dependencies against the runtime's Prefect version.
5. Uses `uv pip sync` to install the generated requirements into uv's selected
   environment. This can remove packages absent from those requirements.

This process deliberately does **not** install from `uv.lock`. The lockfile is
used by local project commands such as `uv sync` and `uv run`; committing it alone
does not change managed-pool installation behavior. Likewise, the top-level
`prefect-version` in `prefect.yaml` does not pin the managed image or runtime.

The script falls back to compiling without the exact Prefect constraint if the
first compilation fails. Its message assumes the image version is not yet on
PyPI, but the fallback currently runs on **any** compilation failure. This is an
existing availability tradeoff, not a guarantee that Prefect remains unchanged.
Investigate the actual resolver error before changing this behavior.

This is a Linux container preparation script, not a Windows local setup command.

## PostgreSQL driver decision

The application is moving from `psycopg2-binary` to
`psycopg[binary]>=3.2,<4`. The binary extra includes PostgreSQL client libraries
without requiring a separate system libpq installation.

SQLAlchemy 2.1 changed the default driver for bare `postgresql://` URLs from
Psycopg 2 to Psycopg 3. A managed run failed while importing `psycopg` when only
Psycopg 2 was declared. This default change is a plausible explanation; the
failing runtime's SQLAlchemy version was not verified in this session.
See the [SQLAlchemy migration notes](https://docs.sqlalchemy.org/en/21/changelog/migration_21.html#default-postgresql-driver-changed-to-psycopg-psycopg-3).

Use explicit `postgresql+psycopg://` URLs for Psycopg 3, including local settings
and the database Secret in Prefect Cloud. `initialize_db_config()` in `flows.py`
loads a Prefect Secret and overrides `settings.POSTGRES_DB_URI`, so changing only
an environment variable may not change the effective database URL. Check the
current Secret name in the code; local development may intentionally alter it.
Do not print database URLs or credentials while diagnosing this.

The inspected database code uses SQLModel/SQLAlchemy sessions, models, queries,
and PostgreSQL upserts, without direct Psycopg 2 cursor APIs. Switching drivers
is not expected to require ORM or schema changes. Actual database reads and
upserts still need verification; import success alone does not prove they work.

## Guidance for future changes

- Prefer a targeted dependency or driver fix when the current runtime adaptation
  works. Do not replace it with a fully locked environment solely because a
  lockfile exists.
- Before proposing a pinned image, inspect the actual pool configuration and
  confirm it supports the requested official image version. Do not claim the
  pool always upgrades Prefect or that its version can always be pinned.
- A locked requirements installation must account for Prefect and its shared
  dependencies already in the runtime. Omitting only Prefect does not eliminate
  dependency conflicts.
- If changing `uv pip sync`, preserve installation into the interpreter that
  executes the flow and consider packages needed by the managed runtime.
- Keep unrelated deployment and ORM behavior intact when fixing this driver
  issue. The user prefers retaining the managed pool and the current setup.

After editing dependencies locally, `uv sync` updates `uv.lock` and `.venv`.
Use these checks for the Psycopg 3 installation without connecting to a database:

```powershell
uv sync
uv run python -c "import psycopg; print('Psycopg:', psycopg.__version__)"
uv run python -c "from sqlalchemy import create_engine; e = create_engine('postgresql+psycopg://'); print('Driver:', e.dialect.driver)"
```

Expect a Psycopg 3 version and `Driver: psycopg`. These are local checks, not
verification of the managed runtime. Commit dependency changes with the updated
lockfile and verify a managed run after the remote source is updated.
