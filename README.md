# PCR Results Pipeline

This is a simple pipeline for processing PCR result files from different directories and formats. The goal is to normalize and standardize the data so it can be easily analyzed downstream.

---

## Overview

The pipeline handles three different types of input files and normalizes them into a standard format:

- **StepOne files**  
  - Handles Cyrillic characters  
  - Extracts and processes metadata  
  - Drops unnecessary columns  

- **QS6 files**  
  - Processes metadata  
  - Drops unnecessary columns  

- **PRO files**  
  - Normalizes sample names  
  - Drops unnecessary columns  

---

## Directory Structure

- `input/` — raw input files  
- `input/processed/` — processed intermediate files  
- `cleaned/` — cleaned and normalized output files  
- `warehouse/` — final aggregated storage  

All necessary folders are created automatically if they don't exist.

---

## Key Functions

- `parse_files()`  
  - Reads files from `input/`  
  - Extracts metadata from lines starting with `* ` or `# `  
  - Finds the `[Results]` section and loads it as a pandas DataFrame  
  - Calls a function to standardize the DataFrame  

- `standardize_meta(metadata: dict)`  
  - Placeholder function to normalize metadata fields  

- Standardization functions for each file type (not fully implemented in pseudocode)  

---

## Requirements

- Python 3.8+  
- pandas  
- prefect (for orchestration, if used)

You can install dependencies with:

```bash
pip install pandas prefect

```
---

## License
 -  MIT
