import sys
import pandas as pd
from pysradb.sraweb import SRAweb

# usage: python get_runs.py SRPxxxxxx
if len(sys.argv) < 2:
    raise SystemExit("Usage: python get_runs.py SRPxxxxxx")

project = sys.argv[1]

db = SRAweb()
meta = db.metadata(project, detailed=True)
runs = meta["run_accession"].dropna().unique()

# Prend juste 5 runs pour un 'bout' du dataset
subset_runs = runs[:5]

pd.Series(subset_runs).to_csv("runs.txt", index=False, header=False)
print("Saved runs.txt with:", subset_runs.tolist())
