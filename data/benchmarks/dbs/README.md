# Benchmark Database Setup

Some benchmarks require databases for evaluation that cannot be included in this repository due to their size. This guide provides setup instructions for all supported databases.

## Overview

The following databases are required for the benchmarks:

| Database | Benchmarks Using It | Type | Setup Required |
|----------|-------------------|------|----------------|
| **BIRD Mini-Dev (SQLite)** | `bird_mini_dev_sqlite`, `bird_mini_dev_sqlite_test_50` | SQLite | Download `dev_databases` folder |
| **BIRD Mini-Dev (PostgreSQL)** | `bird_mini_dev_postgres`, `bird_mini_dev_postgres_test_50` | PostgreSQL | PostgreSQL server + import SQL dump + connection string |
| **Spider 1.0** | `spider_dev`, `spider_realistic`, `spider_dev_test_50` | SQLite | Download `database` folder |
| **Archer** | `archer_en_dev`, `archer_en_dev_test_10` | SQLite | Download database files |
| **Beaver** | `beaver`, `beaver_test_10` | MySQL | MySQL server + connection string |
| **FIBEN** | `fiben` | PostgreSQL | PostgreSQL server + load DDL & CSV data + connection string |

**Note:** Test benchmarks (e.g., `bird_mini_dev_sqlite_test_50`) use the same databases as their full counterparts but with smaller question subsets.

---

## BIRD Mini-Dev (SQLite)

**Used by:** `bird_mini_dev_sqlite`, `bird_mini_dev_sqlite_test_50`

### Steps:

1. **Download the Complete Package:**

   Visit the official BIRD Mini-Dev repository:
   👉 [https://github.com/bird-bench/mini_dev](https://github.com/bird-bench/mini_dev)

   Follow the instructions in the README to download the **"BIRD Mini-Dev Complete Package"**, which includes the required `dev_databases` folder. As of Dec 5, 2025, the direct download link is: https://drive.google.com/file/d/13VLWIwpw5E3d5DUkMvzw7hvHE67a4XkG/view?usp=sharing

2. **Extract the Downloaded Package:**

   After downloading, extract the archive (e.g., `.zip` or `.tar.gz`) to a location of your choice.

3. **Copy the `dev_databases` Folder:**

   From the extracted contents, copy the `dev_databases` folder into the `bird` folder under `data/benchmarks/dbs`.

   ```
   data/benchmarks/dbs/bird/dev_databases/
   ```

---

## BIRD Mini-Dev (PostgreSQL)

**Used by:** `bird_mini_dev_postgres`, `bird_mini_dev_postgres_test_50`

### Option 1: Docker (Recommended)

Run PostgreSQL in Docker for a quick setup:

```bash
# Start PostgreSQL container
docker run --name bird-db -e POSTGRES_PASSWORD=yourpass123 -p 5432:5432 -d postgres

# Create database
docker exec -i bird-db psql -U postgres -c "CREATE DATABASE bird;"

# Import SQL dump (from extracted BIRD Mini-Dev package)
docker exec -i bird-db psql -U postgres -d bird < minidev/MINIDEV_postgresql/BIRD_dev.sql
```

Set environment variable for connection:
```bash
export POSTGRES_CONNECTION_STRING=postgresql://postgres:yourpass123@localhost:5432/bird
```

### Option 2: Local PostgreSQL Installation

Install and start PostgreSQL on your system:

```bash
# MacOS
brew install postgresql
brew services start postgresql

# Linux
sudo apt update && sudo apt install postgresql postgresql-contrib
sudo service postgresql start

# Windows
# Download and run the installer from https://www.postgresql.org/download/windows/
```

Create database and import data:
```bash
# Create database
psql postgres
createdb bird
\q

# Import SQL dump (from extracted BIRD Mini-Dev package)
psql bird < minidev/MINIDEV_postgresql/BIRD_dev.sql
```

Set environment variable for connection:
```bash
export POSTGRES_CONNECTION_STRING="postgresql://${USER}@localhost:5432/bird"
```

---

## Spider 1.0

**Used by:** `spider_dev`, `spider_realistic`, `spider_dev_test_50`

### Steps:

1. **Download the databases:**
   
   Visit: https://yale-lily.github.io/spider
   
   Direct download link: https://drive.google.com/file/d/1403EGqzIDoHMdQF4c9Bkyl7dZLZ5Wt6J/view?usp=sharing

2. **Extract and copy:**
   
   Decompress the downloaded file and copy the `database` folder into the `spider` folder under `data/benchmarks/dbs`.
   
   ```
   data/benchmarks/dbs/spider/database/
   ```

---

## Archer

**Used by:** `archer_en_dev`, `archer_en_dev_test_10`

### Steps:

1. **Download the databases:**
   
   Direct download link: https://sig4kg.github.io/archer-bench/dataset/database.zip

2. **Extract and copy:**
   
   Extract the zip file and copy the database folders into the `archer` folder under `data/benchmarks/dbs`.
   
   ```
   data/benchmarks/dbs/archer/database/
   ```

---

## Beaver

**Used by:** `beaver`, `beaver_test_10`

Beaver requires a MySQL server. You need to:

1. Set up a MySQL server (local or remote)
2. Import the Beaver database schema and data
3. Set the `MYSQL_CONNECTION_STRING` environment variable

```bash
export MYSQL_CONNECTION_STRING="mysql://username:password@localhost:3306/beaver"
```

Refer to the Beaver benchmark documentation for database setup details: https://peterbaile.github.io/beaver/

---

## FIBEN

**Used by:** `fiben`

FIBEN is a financial natural-language querying benchmark whose gold queries target a single PostgreSQL schema named `FIBEN`. You need to:

1. Download the FIBEN DDL and CSV data
2. Set up a PostgreSQL server (local or Docker) and create the `FIBEN` schema
3. Load the DDL and CSV data
4. Set the `POSTGRES_CONNECTION_STRING` environment variable

### Step 1: Download the FIBEN artifacts

The benchmark ships its DDL (`FIBEN.sql`) and per-table CSV data (`data.zip`) in the upstream repository:

👉 [https://github.com/IBM/fiben-benchmark](https://github.com/IBM/fiben-benchmark)

```bash
git clone https://github.com/IBM/fiben-benchmark.git
cd fiben-benchmark

# data.zip extracts the per-table CSV files into a ./data directory.
# tablelist.txt maps each table name to its <TABLE>.csv file.
unzip data.zip
```

### Step 2 & 3: Start PostgreSQL and load the schema + data

#### Option 1: Docker (Recommended)

```bash
# Start PostgreSQL container
docker run --name fiben-db -e POSTGRES_PASSWORD=yourpass123 -p 5432:5432 -d postgres

# Create the database and the FIBEN schema, and make it the default search path
docker exec -i fiben-db psql -U postgres -c "CREATE DATABASE fiben;"
docker exec -i fiben-db psql -U postgres -d fiben -c 'CREATE SCHEMA IF NOT EXISTS "FIBEN";'
docker exec -i fiben-db psql -U postgres -d fiben -c 'ALTER DATABASE fiben SET search_path TO "FIBEN";'

# Create the tables (FIBEN.sql is PostgreSQL-compatible; the search_path above
# places them in the FIBEN schema). FK constraints are added at the end.
docker exec -i fiben-db psql -U postgres -d fiben < FIBEN.sql

# Load the CSV data (db2 "del" export == comma-delimited CSV, no header row)
for t in $(cat tablelist.txt); do
  echo "Loading $t"
  docker exec -i fiben-db psql -U postgres -d fiben \
    -c "\copy \"FIBEN\".\"$t\" FROM STDIN WITH (FORMAT csv)" < "data/$t.csv"
done
```

Set environment variable for connection:
```bash
export POSTGRES_CONNECTION_STRING=postgresql://postgres:yourpass123@localhost:5432/fiben
```

#### Option 2: Local PostgreSQL Installation

```bash
# Create the database and FIBEN schema
createdb fiben
psql fiben -c 'CREATE SCHEMA IF NOT EXISTS "FIBEN";'
psql fiben -c 'ALTER DATABASE fiben SET search_path TO "FIBEN";'

# Create the tables
psql fiben < FIBEN.sql

# Load the CSV data
for t in $(cat tablelist.txt); do
  echo "Loading $t"
  psql fiben -c "\copy \"FIBEN\".\"$t\" FROM '$(pwd)/data/$t.csv' WITH (FORMAT csv)"
done
```

Set environment variable for connection:
```bash
export POSTGRES_CONNECTION_STRING="postgresql://${USER}@localhost:5432/fiben"
```

> **Note:** The `fiben` benchmark resolves its tables against the `FIBEN` schema (configured via `schema_name` in `benchmarks.json`). Make sure the schema is named exactly `FIBEN` (uppercase) so the gold queries resolve correctly.

---

## Troubleshooting

### SQLite Databases Not Found
- Ensure the database folders are in the correct location under `data/benchmarks/dbs/`
- Check that the folder structure matches the paths in `benchmarks.json` or `test-benchmarks.json`

### PostgreSQL Connection Issues
- Verify PostgreSQL is running: `pg_isready`
- Check the connection string format: `postgresql://user:password@host:port/database`
- Ensure the `POSTGRES_CONNECTION_STRING` environment variable is set

### MySQL Connection Issues
- Verify MySQL is running: `mysqladmin ping`
- Check the connection string format: `mysql://user:password@host:port/database`
- Ensure the `MYSQL_CONNECTION_STRING` environment variable is set

---

## Directory Structure

After setup, your `data/benchmarks/dbs/` directory should look like:

```
data/benchmarks/dbs/
├── README.md (this file)
├── bird/
│   └── dev_databases/          # BIRD SQLite databases
├── spider/
│   └── database/               # Spider SQLite databases
└── archer/
    └── database/               # Archer SQLite databases
```
