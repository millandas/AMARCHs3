import pandas as pd

MODEL_FILE = "Model.csv"
CCLE_FILE = "CCLE_sample_info_file_2012-10-18.txt"
OUTPUT_FILE = "model_ccle_merged.csv"


def main():
    # ---- Load Model.csv ----
    print("Loading Model.csv ...")
    model = pd.read_csv(MODEL_FILE)
    print("Model columns:", list(model.columns))

    # Key columns we care about (you can add more later)
    # We'll keep everything for now and reduce after merge.
    # Important: column names are case-sensitive.
    assert "ModelID" in model.columns, "Expected 'ModelID' in Model.csv"
    assert "CCLEName" in model.columns, "Expected 'CCLEName' in Model.csv"

    # ---- Load CCLE sample info ----
    print("\nLoading CCLE sample info ...")
    # This file is tab-separated
    ccle = pd.read_csv(CCLE_FILE, sep="\t")
    print("CCLE sample info columns:", list(ccle.columns))

    assert "CCLE name" in ccle.columns, "Expected 'CCLE name' in CCLE sample info file"

    # ---- Standardise the join key ----
    # We'll make a clean CCLEName column in CCLE to match Model.CCLEName
    ccle["CCLEName"] = ccle["CCLE name"].astype(str).str.strip()

    # Also strip spaces in Model CSV just in case
    model["CCLEName"] = model["CCLEName"].astype(str).str.strip()

    # ---- Merge on CCLEName ----
    print("\nMerging on 'CCLEName' ...")
    merged = model.merge(ccle, on="CCLEName", how="left")
    print("Merged shape:", merged.shape)

    # Optional: see how many models did NOT find a CCLE match
    unmatched = merged[merged["Cell line primary name"].isna()]
    print(f"Unmatched models (no CCLE sample info): {len(unmatched)}")

    # ---- Choose a useful subset of columns for inspection ----
    # You can adjust this list as you like.
    keep_cols = [
        # From Model.csv
        "ModelID",
        "PatientID",
        "CellLineName",
        "StrippedCellLineName",
        "DepmapModelType",
        "OncotreeLineage",
        "OncotreePrimaryDisease",
        "OncotreeSubtype",
        "CCLEName",

        # From CCLE sample info
        "Cell line primary name",
        "Gender",
        "Site Primary",
        "Histology",
        "Hist Subtype1",
        "Notes",
        "Source",
    ]

    # Only keep columns that actually exist (prevents KeyError)
    keep_cols = [c for c in keep_cols if c in merged.columns]
    final = merged[keep_cols].copy()

    print("\nColumns in final merged table:")
    print(final.columns.tolist())

    print("\nSaving merged table to:", OUTPUT_FILE)
    final.to_csv(OUTPUT_FILE, index=False)
    print("Done.")


if __name__ == "__main__":
    main()
