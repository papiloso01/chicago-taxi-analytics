"""Download the pinned Spark 3.5/Scala 2.12 Iceberg runtime and check Maven SHA512."""
import argparse
import hashlib
from pathlib import Path
from urllib.request import urlopen

VERSION = '1.7.1'
NAME = f'iceberg-spark-runtime-3.5_2.12-{VERSION}.jar'
URL = f'https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/{VERSION}/{NAME}'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--directory', default='/opt/iceberg')
    args = parser.parse_args()
    path = Path(args.directory) / NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    with urlopen(URL, timeout=120) as response:
        body = response.read()
    with urlopen(URL + '.sha512', timeout=30) as response:
        expected = response.read().decode().split()[0]
    if hashlib.sha512(body).hexdigest() != expected:
        raise SystemExit('Iceberg runtime SHA512 mismatch')
    path.write_bytes(body)
    print(path)


if __name__ == '__main__':
    main()
