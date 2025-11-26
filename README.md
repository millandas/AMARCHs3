# Gene Expression Data Processing Pipeline

This repository contains a modular Python pipeline for fetching, processing, and storing gene expression datasets from NCBI GEO (Gene Expression Omnibus) directly to AWS S3. It streamlines extraction, transformation, and aggregation of per-sample data for large genomics studies.

---

## Screenshots

<div align="center">

<img src="static/Screenshot 2025-11-27 at 00.27.30.png" alt="Screenshot 1" width="600"/>

*Screenshot 1 - November 27, 2025 at 00:27:30*

<img src="static/Screenshot 2025-11-27 at 00.28.00.png" alt="Screenshot 2" width="600"/>

*Screenshot 2 - November 27, 2025 at 00:28:00*

<img src="static/Screenshot 2025-11-27 at 00.28.18.png" alt="Screenshot 3" width="600"/>

*Screenshot 3 - November 27, 2025 at 00:28:18*

<img src="static/Screenshot 2025-11-27 at 00.28.42.png" alt="Screenshot 4" width="600"/>

*Screenshot 4 - November 27, 2025 at 00:28:42*

</div>

---

## Features

- **Fetch GEO Datasets**: Download and parse metadata and gene expression matrices from NCBI GEO using [GEOparse](https://github.com/guma44/GEOparse).
- **Sample-level Processing**: Extract, write, and upload per-person expression files and attach key metadata as S3 object metadata for searchability.
- **Parallel Processing**: Fast batch processing using multi-threading.
- **S3 Storage**: Direct upload/download to Amazon S3 with configurable bucket and region.
- **Data Aggregation**: Merge all samples into a single tidy DataFrame for downstream analysis, with all per-sample metadata as columns.
- **Pluggable Configuration**: Manage datasets and S3 settings via YAML and environment variables.
- **Supports Multiple Output Formats**: Save merged datasets as CSV or Parquet.

---

## Getting Started

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd <your-repo-directory>
```

### 2. Requirements

- Python 3.8+
- AWS account and S3 bucket (access keys)
- [Pip packages](#installation)

### 3. Setup

#### a. Install dependencies

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install GEOparse boto3 pyyaml python-dotenv pandas
```

#### b. Configure AWS credentials

Create a `.env` file in the root with:

```
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION_NAME=your-region
AWS_S3_BUCKET=your-bucket-name
```

#### c. Dataset Configuration

Edit `config/datasets.yaml` to declare default S3 bucket/region if different from `.env` file:

```yaml
s3_bucket: "your-bucket"
s3_region: "eu-north-1"
```

---

## Usage

### Fetch datasets from GEO and upload to S3

The `scripts/fetch_geo.py` script automates download, metadata extraction, and S3 upload for all samples:

```bash
python scripts/fetch_geo.py
```

- Processes a list of datasets (edit within the script)
- Supports parallel processing (`parallel=True`)
- Per-sample metadata (age, sex, etc.) required for inclusion

To fetch a single dataset (from Python):

```python
from scripts.fetch_geo import GEOFetcher

fetcher = GEOFetcher()
fetcher.fetch_dataset('GSE58137', parallel=True, max_workers=4)
```

Sample files are saved on S3 under `raw/{GEO_ID}/samples/`.

---

### Build and aggregate the final dataset

The `scripts/data_loader.py` script downloads, merges, and saves a single tidy dataset:

```bash
python scripts/data_loader.py
```

This will:
- Retrieve all sample files for a dataset
- Merge and transpose (samples=rows, genes=columns)
- Attach metadata as columns
- Save the aggregated dataset on S3 (`processed/{GEO_ID}/merged_dataset.csv` or `.parquet`)

You can adjust GEO IDs and formats in the script or call programmatically:

```python
from scripts.data_loader import GeneExpressionDataLoader

loader = GeneExpressionDataLoader()
df = loader.build_dataset('GSE58137')
loader.save_dataset(df, 'GSE58137', output_format='csv')
```

---

## File Structure

- `scripts/fetch_geo.py` — GEO download and per-sample extraction
- `scripts/data_loader.py` — Aggregation and DataFrame merging
- `config/datasets.yaml` — Dataset/S3 config
- `.env` — AWS credentials (not committed, see `.gitignore`)

---

## FAQ

- **What S3 permissions are required?**
  - `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` for your bucket.
- **Can I run this without S3?**
  - Minor adaptation required — see code for local path options.
- **Supported datasets?**
  - Any GEO platform with tabular, per-sample files (test with `GSE58137`).

---

## Citation

If you use this pipeline, please cite the repository and original GEO papers.

---

## License

MIT License (see `LICENSE` file for details).

---

