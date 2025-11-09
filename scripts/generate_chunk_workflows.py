import os
import glob

WORKFLOW_DIR = ".github/workflows/chunks"
CHUNK_DIR = "output/chunk"

os.makedirs(WORKFLOW_DIR, exist_ok=True)

template = """name: Deep Validation Chunk {n}

on:
  workflow_dispatch:

jobs:
  deep_chunk_{n}:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repo
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Run deep validation on chunk {n}
        run: |
          python scripts/stage2_deep_validation.py --input {chunk_file}
"""

chunk_files = sorted(glob.glob(os.path.join(CHUNK_DIR, "*.csv")))

for i, f in enumerate(chunk_files, start=1):
    wf_path = os.path.join(WORKFLOW_DIR, f"deep_chunk_{i}.yml")
    with open(wf_path, "w", encoding="utf-8") as w:
        w.write(template.format(n=i, chunk_file=f))
    print(f"✅ Created: {wf_path}")
