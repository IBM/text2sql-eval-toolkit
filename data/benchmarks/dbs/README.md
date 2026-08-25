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

Beaver needs a MySQL server holding several databases; `db_id` is substituted
into the connection string per record, so the database named in
`MYSQL_CONNECTION_STRING` is only a default.

### Steps

1. **Obtain the dumps** from the Beaver project:
   https://peterbaile.github.io/beaver/ — currently `dw.sql`, `nova.sql`, and
   `neutron.sql` (~2.1 GB in total).

2. **Start a MySQL server** (8.x). Locally:
   ```bash
   brew install mysql && brew services start mysql
   ```
   Or use the `mysql` service in `deploy/docker-compose.yml`.

3. **Load them**, which also renames two databases to the names the questions
   use:
   ```bash
   BEAVER_DUMP_DIR=/path/to/beaver_db MYSQL_USER=root ./deploy/load-beaver.sh
   ```

4. **Point the toolkit at the server:**
   ```bash
   export MYSQL_CONNECTION_STRING="mysql://user:password@127.0.0.1:3306/dw"
   ```

### Why the loader renames things

The dumps are named after the source systems, but the benchmark addresses two of
them by a prefixed name. Loading the dumps unchanged produces databases the
benchmark cannot find:

| Dump | Database it creates | Name the benchmark uses |
|---|---|---|
| `dw.sql` | `dw` | `dw` |
| `nova.sql` | `nova` | **`csail_stata_nova`** |
| `neutron.sql` | `neutron` | **`csail_stata_neutron`** |

### Coverage

| Database | Questions | Status |
|---|---|---|
| `dw` | 121 | Loadable |
| `csail_stata_nova` | 43 | Loadable |
| `csail_stata_neutron` | 30 | Loadable |
| `keystone` | 8 | **No dump published** |
| `csail_stata_glance` | 5 | **No dump published** |
| `csail_stata_cinder` | 2 | **No dump published** |

**194 of 209 questions (93%) are executable** with the dumps currently
available; all 194 gold queries have been verified to run against a loaded
server. The remaining 15 fail with an unknown-database error until upstream
publishes those three dumps — a data gap, not a configuration mistake.

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
