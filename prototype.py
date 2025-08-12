import pandas as pd
import os
import shutil
raw_dir = 'input'
processed_dir = 'input/processed'
cleaned_dir = 'cleaned'
warehouse_dir = 'warehouse'

os.makedirs(raw_dir, exist_ok=True)
os.makedirs(processed_dir, exist_ok=True)
os.makedirs(cleaned_dir, exist_ok=True)
os.makedirs(warehouse_dir, exist_ok=True)

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
            df.columns = df.columns.str.lower()
            # empty wells are just unessesary

            df = df.dropna(subset=['sample name'])
            # handle special crylic character u0442
            df.columns = [col.replace('\u0442', 't') for col in df.columns]
            # if well position is not in the column then rename well to well position
            if 'well position' not in df.columns:
                df = df.rename(columns={"well": "well position"})
            # drop well from the other files

            # Replace all case variations of 'undetermined' in ct column with 99.0
            # just to make the interface script esier
            if 'ct' in df.columns:
                df['ct'] = df['ct'].replace(
                    to_replace=r'(?i)^undetermined$', value=99.0, regex=True
                )
                df['ct'] = pd.to_numeric(df['ct'], errors='coerce')

            # this is for the quantstudio pro system where the testnumber is in the comments
            if "comments" not in df.columns:
                df[["comments", "sample name"]] = df["sample name"].str.split("@", n=1, expand=True)

            # drop the samples that does not have a testnumber to not cause error during import
            df = df.dropna(subset=['comments'])

            for key, value in metadata.items():
                df[key] = value
            # this is for the future incase i would like to have a warehouse
            warehouse_path = os.path.join(warehouse_dir, filename.replace('.txt', '_warehouse.csv'))
            df.to_csv(warehouse_path, index=False)  
            print(f"{filename}: {warehouse_path}")
            # this is for the integration
            columns_to_keep = ["well position", "sample name", "target name"]
            cleaned_path = os.path.join(cleaned_dir, filename.replace('.txt', '_cleaned.csv'))
            df.to_csv(cleaned_path, index=False, sep='\t')  
        shutil.move(filepath, os.path.join(processed_dir, filename))
