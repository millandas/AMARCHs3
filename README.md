# AMARCHs3
We decided to re implement Archs4 in python.

## GEO Bulk RNA Fetcher

Use `scripts/fetch_geo_bulk_rna.py` to query NCBI GEO for Illumina HiSeq 2500 bulk RNA-seq series.

```bash
python scripts/fetch_geo_bulk_rna.py --retmax 10 --organism "Homo sapiens"
```

Results are emitted as JSON to stdout by default; pass `--out path.json` to save to a file.
