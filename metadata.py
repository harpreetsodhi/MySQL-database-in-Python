import os
import json

if not os.path.exists("metadata"):
    os.mkdir("metadata")
metadata_path = os.path.join(os.getcwd(), "metadata")
if not os.path.exists("schemas"):
    os.mkdir("schemas")
schema_path = os.path.join(os.getcwd(), "schemas")
for metadata_file in os.listdir(metadata_path):
    os.remove(os.path.join("metadata", metadata_file))
for schema_file in os.listdir(schema_path):
    schema = json.load(open(os.path.join("schemas", schema_file), "r"))
    for key in schema.keys():
        schema[key]["values"] = schema[key]["values"][0:1]
    json.dump(schema, open(os.path.join("metadata", schema_file+"_metadata"), "w+"), indent=2)
print("Metadata generated")
