import pandas as pd
from pathlib import Path
import shutil
from dateutil import parser, tz
from prefect import flow, task
from io import StringIO

RAW_DIR = Path("input")
PROCESSED_DIR = RAW_DIR / "processed"
WAREHOUSE_DIR = Path("warehouse")
ANALYSIS_DIR = Path("analysis_files")
LIMS_IMPORT_DIR = Path("lims_import_files")
ERROR_DIR = RAW_DIR / "error"

# Ensure all dirs exist
for d in [RAW_DIR, PROCESSED_DIR, WAREHOUSE_DIR, ANALYSIS_DIR, LIMS_IMPORT_DIR, ERROR_DIR]:
    d.mkdir(parents=True, exist_ok=True)

REQUIRED_META_KEYS = ["file_name", "instrument_type", "block_type", "run_end_time"]


REQUIRED_DF_COLS = [
    'well position', 'sample name', 'test number', 'target name', 'reporter',
    'ct threshold', 'baseline start', 'baseline end', 'ct'
]


def move_to_error(src: Path, reason: str):
    print(f"{reason} in {src.name}, moving to error.")
    shutil.move(src, ERROR_DIR / src.name)


def extract_filename(fullpath: str) -> str:
    return Path(fullpath).name


def clean_run_endtime(time_str: str):
    tzinfos = {
        "CEST": tz.gettz("Europe/Stockholm"), 
        "CET": tz.gettz("Europe/Stockholm"),
    }
    return pd.to_datetime(parser.parse(time_str, fuzzy=True, tzinfos=tzinfos))


def standardize_meta(metadata: dict):
    key_map = {
        "File Name": "file_name",
        "Experiment File Name": "file_name",
        "Instrument Type": "instrument_type",
        "Instrument Type=": "instrument_type",
        "Block Type": "block_type",
        "Experiment Run End Time": "run_end_time",
        "Run End Data/Time": "run_end_time",
    }
    desired_order = REQUIRED_META_KEYS

    cleaned = {}
    for key, value in metadata.items():
        if key not in key_map:
            continue
        clean_key = key_map[key]
        val = value.strip()
        if clean_key == "file_name":
            val = extract_filename(val)
        elif clean_key == "run_end_time":
            val = clean_run_endtime(val)
        cleaned[clean_key] = val

    return {k: cleaned[k] for k in desired_order if k in cleaned}


def standardize_df(df: pd.DataFrame):
    df.columns = [c.replace("\u0442", "t").lower() for c in df.columns]
    df = df.dropna(subset=['sample name'])

    if 'well position' not in df.columns and 'well' in df.columns:
        df = df.rename(columns={"well": "well position"})
    if "comments" not in df.columns and "sample name" in df.columns:
        df[["comments", "sample name"]] = df["sample name"].str.split("@", n=1, expand=True)
    if 'well' in df.columns:
        df = df.drop('well', axis=1)

    df = df.rename(columns={"comments": "test number"})

    if 'ct' in df.columns:
        df['ct'] = df['ct'].replace(r'(?i)^undetermined$', 99.0, regex=True)
        df['ct'] = pd.to_numeric(df['ct'], errors='coerce')

    return df[REQUIRED_DF_COLS]


def parse_metadata(lines):
    metadata = {}
    for line in lines:
        if line.startswith(('* ', '# ')):
            sep = '=' if '=' in line else ':'
            key, value = line[2:].split(sep, 1)
            metadata[key.strip()] = value.strip()
        if line.strip() == '[Results]':
            break
    return metadata


@task
def process_file(filename: str):
    filepath = RAW_DIR / filename
    lines = filepath.read_text(encoding='utf-8').splitlines(keepends=True)

    metadata = parse_metadata(lines)
    if not metadata:
        return move_to_error(filepath, "No metadata found")

    try:
        results_start = lines.index('[Results]\n') + 1
    except ValueError:
        return move_to_error(filepath, "No [Results] section")

    table_lines = [l for l in lines[results_start:] if l.strip()]
    if not table_lines:
        return move_to_error(filepath, "No data table")

    try:
        df = pd.read_csv(StringIO(''.join(table_lines)), sep='\t')
        df = standardize_df(df)
    except KeyError as e:
        return move_to_error(filepath, f"Missing required column {e}")
    except Exception as e:
        return move_to_error(filepath, f"Unexpected error {e}")

    clean_metadata = standardize_meta(metadata)
    if not all(k in clean_metadata for k in REQUIRED_META_KEYS):
        return move_to_error(filepath, "Metadata missing required keys")

    for k, v in clean_metadata.items():
        df[k] = v

    (ANALYSIS_DIR / f"{filepath.stem}_analysis.txt").write_text(df.to_csv(sep='\t', index=False))
    df[df['test number'].notna()].to_csv(LIMS_IMPORT_DIR / f"{filepath.stem}_import.csv", index=False)
    df[df['test number'].notna()].to_csv(WAREHOUSE_DIR / f"{filepath.stem}_wh.csv", index=False)

    #shutil.move(filepath, PROCESSED_DIR / filename)


@flow
def pcr_processing_flow():
    for filename in RAW_DIR.glob("*.txt"):
        process_file(filename.name)

if __name__ == '__main__':
    pcr_processing_flow()
