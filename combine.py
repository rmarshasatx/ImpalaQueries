import json
import sys

files = sys.argv[1:]

data = {}
for file in files:
  with open(file, "r") as fp:
    file_data = json.load(fp)
    for qid in file_data:
      if qid not in data:
        data[qid] = file_data[qid]

with open("combined.json", "w") as fp:
  json.dump(data, fp)
