# pipline for different dirs psudocode
# required libraries
import pandas as pd
import os
import shutil
import prefect

# function to normalize stepone file
    # handle crylic characters
    # handle metadata
    # drop columns to standard format
    # write to file 
# function to normalize qs6 files
    # handle metadata
    # drop columns to standard format
    # write to file 
# function to normlize pro files
    # handle sample names
    # drop columns to standard format
    # write to file 

raw_dir = 'input'
processed_dir = 'input/processed'
cleaned_dir = 'cleaned'
warehouse_dir = 'warehouse'

os.makedirs(raw_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)
os.makedirs(cleaned_dir, exist_ok=True)
os.makedirs(warehouse_dir, exist_ok=True)

# parse files

def parse_files():
    for filename in os.listdir(raw_dir):
        if filename.endswith('.txt'):
            filepath = os.path.join(raw_dir, filename)
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()

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

            df = standardize_df(df)

            # handle the metadata

            df = standardize_meta(df, metadata)



# standardise metadata
# we want the 
# file name, 
# intrument type, 
# instrument serial number 
# block type,
# run end time (but im not sure if the pcr computers have the correct dates....)

def standardize_meta(df: pd.dataframe , metadata: dict):

    key_map = {
    "File Name": "file_name",
    "Experiment File Name": "file_name",
    "Instrument Type": "instrument_type",
    "Instrument Serial Number": "instrument_serial_number",
    "Block Type": "block_type",
    "Experiment Run End Time": "run_end_time",
    "Run End Data/Time": "run_end_time",
    }
    cleaned_meta = {}
    return metadata


# standardise dataframe used for all types of input files.
def standardize_df(df):

    df.columns = [col.replace('\u0442', 't') for col in df.columns]
    
    df.columns = df.columns.str.lower()

    df = df.dropna(subset=['sample name'])

    if 'well position' not in df.columns:
        df = df.rename(columns={"well": "well position"})
    
    if "comments" not in df.columns:
                df[["comments", "sample name"]] = df["sample name"].str.split("@", n=1, expand=True)

    df = df.dropna(subset=['comments'])

    df = df.rename(columns={"comments":"test number"})
    
    if 'ct' in df.columns:
        df['ct'] = df['ct'].replace(
            to_replace=r'(?i)^undetermined$', value=99.0, regex=True
        )
        df['ct'] = pd.to_numeric(df['ct'], errors='coerce')

    

    
    return df
