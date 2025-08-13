import yaml
import pandas as pd
from pathlib import Path
import shutil
from dateutil import parser, tz
from io import StringIO
import logging
import time

# Load config
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)
# Setup directories from config
RAW_DIR = Path(config["directories"]["raw_dir"])
PROCESSED_DIR = Path(config["directories"]["processed_dir"])
WAREHOUSE_DIR = Path(config["directories"]["warehouse_dir"])
ANALYSIS_DIR = Path(config["directories"]["analysis_dir"])
LIMS_IMPORT_DIR = Path(config["directories"]["lims_import_dir"])
ERROR_DIR = Path(config["directories"]["error_dir"])
LOG_DIR = Path(config["directories"]["log_dir"])

# Create directories if missing
for d in [RAW_DIR, PROCESSED_DIR, WAREHOUSE_DIR, ANALYSIS_DIR, LIMS_IMPORT_DIR, ERROR_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=getattr(logging, config["logging"]["level"].upper(),logging.INFO), 
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(config["logging"]["file"]),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

# Config constants
REQUIRED_META_KEYS = config["required_meta_keys"]
REQUIRED_DF_COLS = config["required_df_cols"]
META_KEY_MAP = config["meta_key_map"]
POLL_INTERVAL = config.get("poll_interval_seconds", 5)


def move_to_error(src: Path, reason: str):
    logger.warning(f"Reason: {reason}. Moving '{src.name}' to error directory.")
    shutil.move(src, ERROR_DIR / src.name)

def clean_run_endtime(time_str: str):
    tzinfos = {
        "CEST": tz.gettz("Europe/Stockholm"),
        "CET": tz.gettz("Europe/Stockholm"),
    }
    return pd.to_datetime(parser.parse(time_str, fuzzy=True, tzinfos=tzinfos))

def parse_raw_file(filepath: Path) -> tuple[dict, pd.DataFrame]:
    logger.info(f"Parsing file: {filepath.name}")
    try:
        lines = filepath.read_text(encoding='utf-8').splitlines(keepends=True)
        metadata = {}
        data_lines = []
        in_results_section = False
        
        for line in lines:
            if line.strip() == '[Results]':
                in_results_section = True
                continue
            
            if in_results_section:
                if line.strip():
                    data_lines.append(line)
            else:
                if line.startswith(('* ', '# ')):
                    sep = '=' if '=' in line else ':'
                    key, value = line[2:].split(sep, 1)
                    metadata[key.strip()] = value.strip()
        
        if not metadata:
            raise ValueError("No metadata found")
        if not data_lines:
            raise ValueError("No data table found in [Results] section")
        
        df = pd.read_csv(StringIO(''.join(data_lines)), sep='\t')
        return metadata, df

    except (ValueError, pd.errors.ParserError) as e:
        move_to_error(filepath, str(e))
        raise

def standardize_metadata(metadata: dict) -> dict:
    cleaned = {}
    for key, value in metadata.items():
        if key in META_KEY_MAP:
            clean_key = META_KEY_MAP[key]
            val = value.strip()
            if clean_key == "file_name":
                val = Path(val).name
            elif clean_key == "run_end_time":
                val = clean_run_endtime(val)
            cleaned[clean_key] = val
    
    if not all(k in cleaned for k in REQUIRED_META_KEYS):
        raise ValueError("Metadata missing required keys")

    return {k: cleaned[k] for k in REQUIRED_META_KEYS}

def standardize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.replace("\u0442", "t").lower() for c in df.columns]
    df.columns = df.columns.str.lower()
    df = df.dropna(subset=['sample name'])

    if 'well position' not in df.columns and 'well' in df.columns:
        df = df.rename(columns={"well": "well position"})
    if "comments" not in df.columns and "sample name" in df.columns:
        df[["comments", "sample name"]] = df["sample name"].str.split("@", n=1, expand=True)
    if 'well' in df.columns:
        df = df.drop('well', axis=1)

    df = df.rename(columns={"comments":"test number"})
    
    if 'ct' in df.columns:
        df['ct'] = df['ct'].replace(r'(?i)^undetermined$', 99.0, regex=True)
        df['ct'] = pd.to_numeric(df['ct'], errors='coerce')
    
    return df[REQUIRED_DF_COLS]

def save_and_move_file(df: pd.DataFrame, clean_metadata: dict, filename: str):
    filepath = RAW_DIR / filename
    
    # Add metadata columns to df
    for k, v in clean_metadata.items():
        df[k] = v

    output_stem = Path(filename).stem
    df.to_csv(ANALYSIS_DIR / f"{output_stem}_analysis.txt", sep='\t', index=False)

    filtered_df = df[df['test number'].notna()]
    if not filtered_df.empty:
        filtered_df.to_csv(WAREHOUSE_DIR / f"{output_stem}_wh.csv", index=False)
    if not filtered_df.empty:
        filtered_df = filtered_df.drop(columns=['well position', 'sample name', 'target name', 'file_name','block_type','run_end_time'])
        filtered_df.to_csv(LIMS_IMPORT_DIR / f"{output_stem}_import.csv", index=False)
    shutil.move(filepath, PROCESSED_DIR / filename)
    logger.info(f"Successfully processed and moved '{filename}'.")

def process_file(file_path: Path):
    try:
        metadata, df = parse_raw_file(file_path)
        clean_metadata = standardize_metadata(metadata)
        standardized_df = standardize_dataframe(df)
        save_and_move_file(standardized_df, clean_metadata, file_path.name)
    except Exception as e:
        print(f"Error processing {file_path.name}: {e}")

def watch_folder(poll_interval=POLL_INTERVAL):
    #processed_files = set()
    while True:
        current_files = set(RAW_DIR.glob("*.txt"))
        #new_files = current_files - processed_files
        for file_path in current_files:
            print(f"Found new file: {file_path.name}, processing...")
            process_file(file_path)
            #processed_files.add(file_path)
        time.sleep(poll_interval)

if __name__ == "__main__":
    print("Starting folder watcher...")
    watch_folder()

