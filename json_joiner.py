import json
import os

INPUT_DIR = "27jan_markets"
OUTPUT_FILE = "combined.json"

merged = []

for filename in os.listdir(INPUT_DIR):
    if not filename.endswith(".json"):
        continue

    filepath = os.path.join(INPUT_DIR, filename)

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        merged.append(data)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(merged, f, indent=2)

print(f"Written {len(merged)} objects to {OUTPUT_FILE}")
