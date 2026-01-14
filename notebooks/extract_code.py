import json
import os

notebook_path = r"c:\Users\Administrator\Documents\subway-congestion-analysis\notebooks\추정매출_혼잡도_상관분석.ipynb"
output_path = r"c:\Users\Administrator\Documents\subway-congestion-analysis\notebooks\validation_script.py"

with open(notebook_path, "r", encoding="utf-8") as f:
    nb = json.load(f)

code_cells = []
for cell in nb["cells"]:
    if cell["cell_type"] == "code":
        source = "".join(cell["source"])
        code_cells.append(source)

with open(output_path, "w", encoding="utf-8") as f:
    f.write("\n\n".join(code_cells))

print(f"Extracted {len(code_cells)} code cells to {output_path}")
