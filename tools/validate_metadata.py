from pathlib import Path
import yaml
from chicago_taxi.quality.contracts import load_contracts, data_dictionary

if __name__ == '__main__':
    contracts = load_contracts()
    for folder in ['metadata/sources', 'metadata/metrics']:
        for path in Path(folder).glob('*.yml'):
            document = yaml.safe_load(path.read_text())
            if not document or 'description' not in document:
                raise ValueError('Incomplete metadata: ' + str(path))
    data_dictionary(contracts, 'docs/data_dictionary.md')
    print(f'Validated {len(contracts)} table contracts and lineage')
