import csv
import xml.etree.ElementTree as ET
from pathlib import Path

def build_limsml_from_csv(csv_path, output_path):
    # Read CSV into a list of rows
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Group by TEST_NUMBER
    grouped = {}
    for row in rows:
        test_number = str(int(float(row['test number'])))  # normalize to int string
        grouped.setdefault(test_number, []).append(row)

    # Build XML root
    limsml = ET.Element("limsml")
    header = ET.SubElement(limsml, "header")
    body = ET.SubElement(limsml, "body")
    transaction = ET.SubElement(body, "transaction", response_type="system")
    system = ET.SubElement(transaction, "system")
    entity_sample = ET.SubElement(system, "entity", type="SAMPLE")

    # Actions for SAMPLE level
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
        ET.SubElement(fields_test, "field", id="INSTRUMENT", direction="in").text = results[0]['intrument_type']  # You may need actual instrument source

        children_test = ET.SubElement(entity_test, "children")

        # Add RESULT entities for each row
        for row in results:
            reporter = row['reporter']
            # Map columns to LIMSML names
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

    # Errors node at the end
    ET.SubElement(limsml, "errors")

    # Save to file with XML declaration
    tree = ET.ElementTree(limsml)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    tree.write(output_path, encoding='utf-8', xml_declaration=True)


if __name__ == "__main__":
    csv_file = Path("lims_import_files/input/B96_001127 250610 EH S_import.csv")
    output_file = Path("lims_import_files/output/sample.limsml.xml")
    build_limsml_from_csv(csv_file, output_file)
    print(f"LIMSML file created: {output_file}")
