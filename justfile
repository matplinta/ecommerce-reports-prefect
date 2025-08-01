# Justfile

default:
    @just --list

# Run flows.py default
flows:
    uv run flows.py

# Initialize DB with schema
init-db:
    uv run -m src.db.engine

# Reset db - provide postgresql connection string
reset-db dsn:
    psql "{{dsn}}" -v ON_ERROR_STOP=1 -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

# Purge local db
purge-local-db:
    docker compose down -v 
    @sleep 1
    docker compose up -d 

# pg_dump -h 127.0.0.1 -p 5432 -U dev -d shop -Fc --no-owner --no-privileges -f seed.dump
# pg_restore -l seed.dump > toc.list
# pg_restore --data-only --no-owner --no-privileges -L toc.list -j 1 -d "connection string"  seed.dump