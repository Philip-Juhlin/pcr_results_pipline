# pipline for different dirs psudocode
# required libraries
import pandas as pd
import os
import shutil
import prefect
from dateutil import parser, tz

raw_dir = 'input'
processed_dir = 'input/processed'
warehouse_dir = 'warehouse'
analysis_dir = 'analysis_files'
lims_import_dir = 'lims_import_files'

os.makedirs(raw_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)
os.makedirs(warehouse_dir, exist_ok=True)
os.makedirs(analysis_dir, exist_ok=True)
os.makedirs(lims_import_dir, exist_ok=True)

def parse_files():
    for filename in os.listdir(raw_dir):
        if filename.endswith('.txt'):
            filepath = os.path.join(raw_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            print(filepath)
            metadata = {}
            for line in lines:
                if line.startswith('* '):
                    key, value = line[2:].split('=', 1)
                    metadata[key.strip()] = value.strip()
                if line.startswith('# '):
                    key, value = line[2:].split(':', 1)  
                    metadata[key.strip()] = value.strip()
                if line.strip() == '[Results]':
                    break
            try:
                results_start = lines.index('[Results]\n') + 1
            except ValueError:
                continue
            table_lines = lines[results_start:]
            table_lines = [l for l in table_lines if l.strip()]
            
            if table_lines:
                from io import StringIO
                df = pd.read_csv(StringIO(''.join(table_lines)), sep='\t')

                # this does not drop empty test numbers.
                df = standardize_df(df)


                clean_metadata = standardize_meta(metadata)

                for key, value in clean_metadata.items():
                    df[key] = value

                # write to file and move processed files
                # write to analysis dir can contain empty testnumber uses \t in vba macro so use that
                # analysis_path = os.path.join(analysis_dir, filename.replace('.txt', '_analysis.txt'))
                # df.to_csv(analysis_path, index=False, sep='\t') 
                # # write to the import dir cannot contain empty testnumber as the interface will through exception
                # df = df.dropna(subset='test number')
                # import_path = os.path.join(lims_import_dir, filename.replace('.txt', '_import.csv'))
                # df.to_csv(import_path, index=False) 
                # # write to the warehouse directory for future use dont include empty testnumbers
                # warehouse_path = os.path.join(warehouse_dir, filename.replace('.txt', '_wh.csv'))
                # df.to_csv(warehouse_path, index=False) 

                #move processed files
            #shutil.move(filepath, os.path.join(processed_dir, filename))
                            


def extract_filename(fullpath:str) -> str:
    return os.path.basename(fullpath)


def clean_run_endtime(time_str):
    TZINFOS = {
        "CEST": tz.gettz("Europe/Stockholm"), 
        "CET": tz.gettz("Europe/Stockholm"),
    }
    dt = parser.parse(time_str, fuzzy=True, tzinfos=TZINFOS) 
    return pd.to_datetime(dt) 


def standardize_meta(metadata):
    key_map = {
    "File Name": "file_name",
    "Experiment File Name": "file_name",
    "Instrument Type": "instrument_type",
    "Instrument Type=": "instrument_type",
    "Block Type": "block_type",
    "Experiment Run End Time": "run_end_time",
    "Run End Data/Time": "run_end_time",
    }

    desired_order = [
        "file_name",
        "instrument_type",
        "block_type",
        "run_end_time"
    ]

    cleaned_meta_raw = {}
    for key, value in metadata.items():
        if key in key_map:
            clean_key = key_map[key]
            clean_value = value.strip()
            if clean_key == "file_name":
                clean_value = extract_filename(clean_value)
            if clean_key == "run_end_time":
                clean_value = clean_run_endtime(clean_value) 
            cleaned_meta_raw[clean_key] = clean_value

    cleaned_meta = {}
    for k in desired_order:
        if k in cleaned_meta_raw:
            cleaned_meta[k] = cleaned_meta_raw[k]

    return cleaned_meta


def standardize_df(df):

    df.columns = [col.replace('\u0442', 't') for col in df.columns]
    
    df.columns = df.columns.str.lower()

    df = df.dropna(subset=['sample name'])

    if 'well position' not in df.columns:
        df = df.rename(columns={"well": "well position"})
    
    if "comments" not in df.columns:
                df[["comments", "sample name"]] = df["sample name"].str.split("@", n=1, expand=True)

    # might keep empty ones in case manual entry on machine and handle this later in the pipline
    #df = df.dropna(subset=['comments'])

    if 'well' in df.columns:
        df = df.drop('well', axis=1)

    df = df.rename(columns={"comments":"test number"})
    
    if 'ct' in df.columns:
        df['ct'] = df['ct'].replace(
            to_replace=r'(?i)^undetermined$', value=99.0, regex=True
        )
        df['ct'] = pd.to_numeric(df['ct'], errors='coerce')

    cols_to_keep = [
        'well position', 
        'sample name', 
        'target name', 
        'test number', 
        'reporter', 
        'ct threshold', 
        'baseline start', 
        'baseline end', 
        'ct']
    df = df[cols_to_keep]
    return df


parse_files()
