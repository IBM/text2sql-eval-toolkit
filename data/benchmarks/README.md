# Benchmarks

This directory contains benchmark datasets and configurations for evaluating text-to-SQL systems. Benchmarks are organized into two categories:

- **Full Benchmarks** ([`benchmarks.json`](../benchmarks.json)) - Full-scale benchmarks for comprehensive evaluation
- **Test Benchmarks** ([`test-benchmarks.json`](../test-benchmarks.json)) - Smaller subsets for quick validation and CI/CD

## Production Benchmarks

Production benchmarks are defined in [`data/benchmarks.json`](../benchmarks.json) and include:

### BIRD Mini-Dev (SQLite)
- **ID:** `bird_mini_dev_sqlite`
- **Description:** BIRD-SQL Mini-Dev in SQLite
- **Size:** 500 questions
- **Database:** SQLite
- **Source:** https://github.com/bird-bench/mini_dev
- **Setup Required:** Download BIRD databases (see [dbs/README.md](dbs/README.md))

### BIRD Mini-Dev (PostgreSQL)
- **ID:** `bird_mini_dev_postgres`
- **Description:** BIRD-SQL Mini-Dev in PostgreSQL
- **Size:** 500 questions
- **Database:** PostgreSQL
- **Source:** https://github.com/bird-bench/mini_dev
- **Setup Required:** PostgreSQL database and `POSTGRES_CONNECTION_STRING` environment variable

### Beaver
- **ID:** `beaver`
- **Description:** Beaver benchmark for business intelligence queries
- **Size:** 209 questions
- **Database:** MySQL
- **Source:** https://peterbaile.github.io/beaver/
- **Setup Required:** MySQL database and `MYSQL_CONNECTION_STRING` environment variable

### Archer (English Dev)
- **ID:** `archer_en_dev`
- **Description:** Archer English Dev Set for knowledge graph queries
- **Size:** 104 questions
- **Database:** SQLite
- **Source:** https://sig4kg.github.io/archer-bench/
- **Setup Required:** Download Archer databases (see [dbs/README.md](dbs/README.md))

### Spider Dev
- **ID:** `spider_dev`
- **Description:** Spider Dev Set - Full benchmark
- **Size:** 1,034 questions across 200 databases
- **Database:** SQLite
- **Source:** https://yale-lily.github.io/spider
- **Setup Required:** Download Spider databases (see [dbs/README.md](dbs/README.md))

### Spider Realistic
- **ID:** `spider_realistic`
- **Description:** Spider Realistic Dataset with paraphrased questions
- **Size:** 508 questions
- **Database:** SQLite
- **Source:** https://zenodo.org/records/5205322
- **Setup Required:** Download Spider databases (see [dbs/README.md](dbs/README.md))

## Test Benchmarks

Test benchmarks are defined in [`data/test-benchmarks.json`](../test-benchmarks.json) and provide smaller subsets for:
- Quick validation during development
- Continuous Integration / Continuous Deployment (CI/CD) pipelines
- Debugging and testing new features
- Rapid iteration without full benchmark overhead

### BIRD Mini-Dev SQLite Test (50)
- **ID:** `bird_mini_dev_sqlite_test_50`
- **Description:** 50-question test sample from BIRD Mini-Dev (SQLite)
- **Size:** 50 questions
- **Database:** SQLite
- **Data:** `benchmarks/test_benchmarks/bird_mini_dev_sqlite_test_50.json`
- **Results:** `benchmarks/test_benchmarks/results/`

### BIRD Mini-Dev PostgreSQL Test (50)
- **ID:** `bird_mini_dev_postgres_test_50`
- **Description:** 50-question test sample from BIRD Mini-Dev (PostgreSQL)
- **Size:** 50 questions
- **Database:** PostgreSQL
- **Data:** `benchmarks/test_benchmarks/bird_mini_dev_postgres_test_50.json`
- **Results:** `benchmarks/test_benchmarks/results/`

### Spider Dev Test (50)
- **ID:** `spider_dev_test_50`
- **Description:** 50-question test sample from Spider Dev
- **Size:** 50 questions
- **Database:** SQLite
- **Data:** `benchmarks/test_benchmarks/spider_dev_test_50.json`
- **Results:** `benchmarks/test_benchmarks/results/`

### Beaver Test (10)
- **ID:** `beaver_test_10`
- **Description:** 10-question test sample from Beaver
- **Size:** 10 questions
- **Database:** MySQL
- **Data:** `benchmarks/test_benchmarks/beaver_test_10.json`
- **Results:** `benchmarks/test_benchmarks/results/`

### Archer English Dev Test (10)
- **ID:** `archer_en_dev_test_10`
- **Description:** 10-question test sample from Archer English Dev
- **Size:** 10 questions
- **Database:** SQLite
- **Data:** `benchmarks/test_benchmarks/archer_en_dev_test_10.json`
- **Results:** `benchmarks/test_benchmarks/results/`

### BIRD SQLite Test Benchmark (3)
- **ID:** `bird_sqlite_test_benchmark`
- **Description:** Minimal 3-question benchmark for code validation
- **Size:** 3 questions
- **Database:** SQLite (self-contained in `test_benchmarks/db/`)
- **Data:** `benchmarks/test_benchmarks/bird_sqlite_test_benchmark.json`
- **Results:** `benchmarks/test_benchmarks/results/`
- **Note:** This is the smallest benchmark, ideal for quick smoke tests

## Benchmark Configuration Format

Both `benchmarks.json` and `test-benchmarks.json` follow the same structure:

```json
{
    "benchmark_id": {
        "name": "benchmark_id",
        "description": "Human-readable description with source URL",
        "data": "path/to/questions.json",
        "schema": "path/to/schema.json",
        "predictions": "path/to/predictions.json",
        "db_engine": {
            "db_type": "sqlite|postgres|mysql",
            "db_folder": "path/to/databases",  // For SQLite
            "schema_name": "public",           // For PostgreSQL
            "connection_string_env_var": "ENV_VAR_NAME"  // For PostgreSQL/MySQL
        }
    }
}
```

## Database Setup

Most benchmarks require downloading databases or setting up database connections. See [dbs/README.md](dbs/README.md) for detailed setup instructions for:
- BIRD Mini-Dev databases
- Spider databases
- Archer databases
- PostgreSQL and MySQL connection setup

## Usage Examples

### Running a Production Benchmark
```bash
python scripts/run_experiment.py bird_mini_dev_sqlite
```

### Running a Test Benchmark
```bash
python scripts/run_experiment.py bird_sqlite_test_benchmark
```

### Running All Test Benchmarks
```bash
python scripts/run_all_benchmarks.py --test
```

### Model Configuration

The `run_all_benchmarks.py` script supports separate model configurations for standard and agentic baselines:

- **Standard Baseline Models** (`--model_names`): Used for fast, single-shot SQL generation
  - Default: 4 models (llama-3-3-70b, granite-4-h-small, llama-4-maverick-17b, gpt-oss-120b)
  - Fast execution, suitable for testing multiple models

- **Agentic Baseline Models** (`--agentic_models`): Used for multi-step agentic SQL generation
  - Default: 1 model (gpt-oss-120b)
  - Slower execution due to multiple LLM calls per question
  - Optimized for efficiency by default

**Example: Running all baselines with default configuration**
```bash
# Standard baseline: 4 models, Agentic baselines: 1 model (10 total pipeline runs)
python scripts/run_all_benchmarks.py bird_sqlite_test_benchmark --test --run_all_baselines
```

**Example: Custom model configuration**
```bash
# Override agentic models to test multiple models
python scripts/run_all_benchmarks.py bird_sqlite_test_benchmark --test --run_all_baselines \
  --agentic_models "wxai:meta-llama/llama-3-3-70b-instruct" "wxai:ibm/granite-4-h-small"

# Override standard baseline models
python scripts/run_all_benchmarks.py bird_sqlite_test_benchmark --test --run_all_baselines \
  --model_names "wxai:openai/gpt-oss-120b"
```

**Performance Note:** When using `--run_all_baselines`, the script runs:
- 1 standard baseline × N models (from `--model_names`)
- 6 agentic variants × M models (from `--agentic_models`)

With defaults (N=4, M=1), this results in 10 pipeline runs instead of 28 (7 pipelines × 4 models), significantly reducing runtime while maintaining comprehensive coverage.

## Results

Evaluation results are stored in:
- **Production benchmarks:** `data/results/` with dashboard at `data/results/README.md`
- **Test benchmarks:** `data/benchmarks/test_benchmarks/results/` with dashboard at `data/benchmarks/test_benchmarks/results/README.md`

## Adding New Benchmarks

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines on adding new benchmarks to the toolkit.