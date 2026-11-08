"""Build the pure Python Spark executor dependency zip, including JSON contracts."""
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

root = Path(__file__).resolve().parents[1]
output = root / 'data/lending/build/lending_cdc.zip'
output.parent.mkdir(parents=True, exist_ok=True)
with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
    for path in sorted((root / 'src/lending_cdc').rglob('*')):
        if path.suffix in ('.py', '.json') and '__pycache__' not in path.parts:
            archive.write(path, path.relative_to(root / 'src'))
print(output)
