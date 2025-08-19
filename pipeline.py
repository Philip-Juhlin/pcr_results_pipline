import yaml
import pandas as pd
from pathlib import Path
import shutil
from dateutil import parser, tz
from io import StringIO
import os
import logging
import time
import xml.etree.ElementTree as ET
CONFIG_DIR = Path(__file__).parent.resolve()
config_path = CONFIG_DIR / "config.yaml"
# Load config
with open(config_path, "r") as f:
    config = yaml.safe_load(f)
# make a single base dir so the others are relative
BASE_DIR = Path(config["directories"]["base_dir"])
# Setup directories from config
RAW_DIR = BASE_DIR / (config["directories"]["raw_dir"])
PROCESSED_DIR = BASE_DIR / (config["directories"]["processed_dir"])
WAREHOUSE_DIR = BASE_DIR / (config["directories"]["warehouse_dir"])
ANALYSIS_DIR = BASE_DIR / (config["directories"]["analysis_dir"])
LIMS_IMPORT_DIR = BASE_DIR / (config["directories"]["lims_import_dir"])
ERROR_DIR = BASE_DIR / (config["directories"]["error_dir"])
LOG_DIR = BASE_DIR / (config["directories"]["log_dir"])

# Create directories if missing
for d in [RAW_DIR, PROCESSED_DIR, WAREHOUSE_DIR, ANALYSIS_DIR, LIMS_IMPORT_DIR, ERROR_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=getattr(logging, config["logging"]["level"].upper(),logging.INFO), 
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / config["logging"]["file"]),
        logging.StreamHandler()
    ])
logger = logging.getLogger(__name__)

# Config constants
REQUIRED_META_KEYS = config["required_meta_keys"]
REQUIRED_DF_COLS = config["required_df_cols"]
META_KEY_MAP = config["meta_key_map"]
INSTRUMENT_KEY_MAP = config["instrument_key_map"]
POLL_INTERVAL = config.get("poll_interval_seconds", 5)


def move_to_error(src: Path, reason: str):
    logger.error(f"Reason: {reason}. Moving '{src.name}' to error directory.")
    shutil.move(src, ERROR_DIR / src.name)

def instrument_name(df: pd.DataFrame) -> pd.DataFrame:
    # Replace values in the column where they match the keys
    if 'instrument_type' in df.columns:
        df['instrument_type'] = df['instrument_type'].map(INSTRUMENT_KEY_MAP).fillna(df['instrument_type'])

    return df

def merge_df_metadate(df: pd.DataFrame, clean_metadata: dict):
    
    # Add metadata columns to df
    for k, v in clean_metadata.items():
        df[k] = v
    return df

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
    if 'well' not in df.columns and 'well position' in df.columns:
        df['well'] = df['well position']

    df = df.rename(columns={"comments":"test number"})
    
    if 'ct' in df.columns:
        df['ct'] = df['ct'].replace(r'(?i)^undetermined$', 99.0, regex=True)
        df['ct'] = pd.to_numeric(df['ct'], errors='coerce')
    cols = ['test number', 'baseline start', 'baseline end']
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # convert, force errors to NaN
            df[col] = df[col].fillna(0)                         # or choose another default
            df[col] = df[col].astype(int).astype(str)
    return df[REQUIRED_DF_COLS]

def save_and_move_file(df: pd.DataFrame, filename: str):
    filepath = RAW_DIR / filename
    
    output_stem = Path(filename).stem
    df.to_csv(ANALYSIS_DIR / f"{output_stem}_analysis.txt", sep='\t', index=False)

    filtered_df = df[df['test number'].notna()]
    if not filtered_df.empty:
        filtered_df.to_csv(WAREHOUSE_DIR / f"{output_stem}_wh.csv", index=False)
        lims_df = filtered_df.drop(columns=['sample name', 'target name','well', 'well position', 'file_name', 'block_type',	'run_end_time'])
        lims_df = instrument_name(lims_df)
        lims_df.to_csv(LIMS_IMPORT_DIR/f"{output_stem}_lims.txt", sep="\t", index=False)
    shutil.move(filepath, PROCESSED_DIR / filename)
    logger.info(f"Successfully processed and moved '{filename}'.")

def build_limsml(df: pd.DataFrame, filename: str):
    df = df.copy()
   # Ensure test number is normalized
    df['test number'] = df['test number'].astype(float).astype(int).astype(str)

    df = instrument_name(df)

    # Group by TEST_NUMBER
    grouped = {tn: group.to_dict(orient='records') 
               for tn, group in df.groupby('test number')}

    # Build XML
    limsml = ET.Element("limsml")
    header = ET.SubElement(limsml, "header")
    body = ET.SubElement(limsml, "body")
    transaction = ET.SubElement(body, "transaction", response_type="system")
    system = ET.SubElement(transaction, "system")
    entity_sample = ET.SubElement(system, "entity", type="SAMPLE")

    # SAMPLE-level actions
    actions = ET.SubElement(entity_sample, "actions")
    action = ET.SubElement(actions, "action")
    ET.SubElement(action, "command").text = "RESULT_ENTRY"
    ET.SubElement(action, "parameter", name="ANAL_TRAIN_REASON").text = "TRUE"
    ET.SubElement(action, "parameter", name="INST_TRAIN_REASON").text = "TRUE"

    children_sample = ET.SubElement(entity_sample, "children")

    # Add TEST entities
    for test_number, results in grouped.items():
        entity_test = ET.SubElement(children_sample, "entity", type="TEST")
        ET.SubElement(entity_test, "actions")
        fields_test = ET.SubElement(entity_test, "fields")
        ET.SubElement(fields_test, "field", id="TEST_NUMBER", direction="in").text = test_number
        ET.SubElement(fields_test, "field", id="INSTRUMENT", direction="in").text = results[0]['instrument_type']

        children_test = ET.SubElement(entity_test, "children")

        # Add RESULT entities
        for row in results:
            reporter = row['reporter']
            field_map = {
                f"Ct Threshold_{reporter}": row['ct threshold'],
                f"Baseline Start_{reporter}": row['baseline start'],
                f"Baseline End_{reporter}": row['baseline end'],
                f"Ct_{reporter}": row['ct']
            }
            for name, value in field_map.items():
                entity_result = ET.SubElement(children_test, "entity", type="RESULT")
                ET.SubElement(entity_result, "actions")
                fields_result = ET.SubElement(entity_result, "fields")
                ET.SubElement(fields_result, "field", id="NAME", direction="in").text = name
                ET.SubElement(fields_result, "field", id="TEXT", direction="in").text = str(value)

    ET.SubElement(limsml, "errors")

    output_path = LIMS_IMPORT_DIR/f"{Path(filename).stem}.limsml.xml"
    # Save XML
    tree = ET.ElementTree(limsml)
    tree.write(output_path, encoding='utf-8', xml_declaration=True)
    logger.info(f"LIMSML file created: {output_path}")


def process_file(file_path: Path):
    try:
        metadata, df = parse_raw_file(file_path)
        clean_metadata = standardize_metadata(metadata)
        standardized_df = standardize_dataframe(df)
        merged_df = merge_df_metadate(standardized_df, clean_metadata)

        #construct limsml file
        # filtered_df = merged_df[merged_df['test number'].notna()]
        # if not filtered_df.empty:
        #     build_limsml(filtered_df, file_path.name)

        # save analyis and warehouse files and move rawfile 
        save_and_move_file(merged_df, file_path.name)

    except Exception as e:
        move_to_error(file_path, str(e))

def watch_folder(poll_interval=POLL_INTERVAL, stop_file=None ):
    #processed_files = set().
    while True:
        if stop_file and Path(stop_file).exists():
            logger.info("Stopping folder watcher...")
            break
        current_files = set(RAW_DIR.glob("*.txt"))
        #new_files = current_files - processed_files
        for file_path in current_files:
            print(f"Found new file: {file_path.name}, processing...")
            process_file(file_path)
            #processed_files.add(file_path)
        time.sleep(poll_interval)

if __name__ == "__main__":
    stop_file = Path("stop.flag")
    logger.info("Starting folder watcher...")
    watch_folder(stop_file=stop_file)

