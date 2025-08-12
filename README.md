# PCR Results Processing Pipeline

This is a Python-based pipeline for processing PCR result files dropped into an input folder.  
It parses metadata and tabular data, normalizes and standardizes the results, and outputs cleaned files for further analysis or import.

---

## Features

- Watches the `input/` folder continuously and processes new `.txt` files as they appear.  
- Extracts metadata and result tables from raw files with flexible parsing.  
- Standardizes metadata keys and DataFrame columns for consistent downstream use.  
- Handles timezone-aware parsing of run end times.  
- Moves processed files to `input/processed/` and problematic files to `input/error/`.  
- Saves outputs in multiple target directories for analysis, warehouse storage, and LIMS imports.

---

## Directory Structure

- input/ # Incoming raw files to process
- input/processed/ # Successfully processed files moved here
- input/error/ # Files moved here if processing fails
- warehouse/ # Final warehouse storage CSVs
- analysis_files/new/ # Analysis-ready tab-delimited files
- lims_import_files/input/ # Files formatted for LIMS import

Directories are created automatically if missing.

---

## How It Works

1. The script watches the `input/` folder every 5 seconds (default poll interval).  
2. Any `.txt` files found are parsed to extract metadata and the `[Results]` section as a DataFrame.  
3. Metadata is cleaned and standardized (e.g., normalizing timezones, file names).  
4. The DataFrame columns are normalized (case-insensitive, column renames, missing values handled).  
5. Cleaned data is saved to the analysis, warehouse, and LIMS import folders.  
6. The original input file is moved to `input/processed/`.  
7. Files failing any step are moved to `input/error/` with a logged reason.

---

## Requirements

- Python 3.8+  
- pandas  
- python-dateutil  

Install dependencies with:

```bash
pip install pandas python-dateutil
```
---

## License
 -  MIT
