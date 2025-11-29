## ==============================
## Install & load PharmacoGx
## ==============================
if (!requireNamespace("BiocManager", quietly = TRUE)) {
  install.packages("BiocManager")
}
if (!requireNamespace("PharmacoGx", quietly = TRUE)) {
  BiocManager::install("PharmacoGx")
}

library(PharmacoGx)

## ==============================
## 1. Download GDSC1 and GDSC2
## ==============================

# This will download preprocessed "PharmacoSet" objects.
# They are big the first time, but then cached locally.
gdsc1 <- downloadPSet("GDSC1")
gdsc2 <- downloadPSet("GDSC2")

## ==============================
## 2. Drug response matrices (IC50, AAC, etc.)
## ==============================

# You can choose different sensitivity measures:
#   "ic50_recomputed", "ic50_pUBLISHED", "aac_recomputed", etc.
# The most commonly used is "ic50_recomputed" or "ic50_published".
# We'll use IC50 recomputed and AAC recomputed for reference.

resp_ic50_gdsc1 <- summarizeSensitivityProfiles(
  gdsc1,
  sensitivity.measure = "ic50_recomputed",
  summary.stat = "median",
  verbose = TRUE
)

resp_ic50_gdsc2 <- summarizeSensitivityProfiles(
  gdsc2,
  sensitivity.measure = "ic50_recomputed",
  summary.stat = "median",
  verbose = TRUE
)

# Optional: AAC if you want an alternative label
resp_aac_gdsc1 <- summarizeSensitivityProfiles(
  gdsc1,
  sensitivity.measure = "aac_recomputed",
  summary.stat = "median",
  verbose = TRUE
)

resp_aac_gdsc2 <- summarizeSensitivityProfiles(
  gdsc2,
  sensitivity.measure = "aac_recomputed",
  summary.stat = "median",
  verbose = TRUE
)

## Matrices are:
##   rows    = drugs
##   columns = cell lines
## We’ll convert them to long tables for easier merging in Python.

library(tidyr)
library(dplyr)

mat_to_long <- function(mat, pset_name) {
  df <- as.data.frame(mat)
  df$drug_id <- rownames(mat)
  long <- tidyr::pivot_longer(
    df,
    cols = -drug_id,
    names_to = "cell_id",
    values_to = "value"
  )
  long$pset <- pset_name
  long
}

ic50_long_1 <- mat_to_long(resp_ic50_gdsc1, "GDSC1")
ic50_long_2 <- mat_to_long(resp_ic50_gdsc2, "GDSC2")

aac_long_1  <- mat_to_long(resp_aac_gdsc1, "GDSC1")
aac_long_2  <- mat_to_long(resp_aac_gdsc2, "GDSC2")

ic50_long <- bind_rows(ic50_long_1, ic50_long_2)
aac_long  <- bind_rows(aac_long_1, aac_long_2)

colnames(ic50_long)[colnames(ic50_long) == "value"] <- "IC50"
colnames(aac_long)[colnames(aac_long) == "value"]  <- "AAC"

## ==============================
## 3. Drug metadata (names, SMILES, etc.)
## ==============================

drug_info_1 <- drugInfo(gdsc1)  # data.frame
drug_info_2 <- drugInfo(gdsc2)

drug_info_1$pset <- "GDSC1"
drug_info_2$pset <- "GDSC2"

drug_info <- bind_rows(drug_info_1, drug_info_2)

## Typically includes columns like:
##   drugid, drug_name, target, putative_target, SMILES, etc.
## (Column names depend on PharmacoGx version; you can check with colnames(drug_info))

## ==============================
## 4. Cell-line metadata
## ==============================

cell_info_1 <- cellInfo(gdsc1)  # AnnotatedDataFrame → data.frame
cell_info_2 <- cellInfo(gdsc2)

cell_info_1$pset <- "GDSC1"
cell_info_2$pset <- "GDSC2"

cell_info <- bind_rows(
  as.data.frame(cell_info_1),
  as.data.frame(cell_info_2)
)

## Typically contains:
##   cell_id, tissueid, cancer_type, cell.line, etc.

## ==============================
## 5. Save everything to CSV
## ==============================

write.csv(ic50_long,
          file = "GDSC_IC50_long.csv",
          row.names = FALSE)

write.csv(aac_long,
          file = "GDSC_AAC_long.csv",
          row.names = FALSE)

write.csv(drug_info,
          file = "GDSC_drug_info.csv",
          row.names = FALSE)

write.csv(cell_info,
          file = "GDSC_cell_info.csv",
          row.names = FALSE)

cat("Saved:\n",
    "- GDSC_IC50_long.csv\n",
    "- GDSC_AAC_long.csv\n",
    "- GDSC_drug_info.csv\n",
    "- GDSC_cell_info.csv\n")
