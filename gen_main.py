import csv
import json
import sys
from bigquery_schema_generator.generate_schema import SchemaGenerator

FILENAME = "d://Datasets//final_project//ratings.tsv"

generator = SchemaGenerator(
    input_format='csvdictreader',
    infer_mode=True,
)
with open(FILENAME) as file:
    reader = csv.DictReader(file, delimiter='\t')
    schema_map, errors = generator.deduce_schema(reader)

schema = generator.flatten_schema(schema_map)
json.dump(schema, sys.stdout, indent=2)
print()