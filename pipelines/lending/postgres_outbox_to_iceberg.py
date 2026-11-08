"""Same entry point in local Docker and AWS Glue; cloud execution is unverified."""
import argparse
import json

from lending_cdc.iceberg import run, session


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--warehouse', required=True)
    parser.add_argument('--source', default='lending-postgres-dev')
    parser.add_argument('--catalog', default='local')
    parser.add_argument('--database', default='lending_cdc')
    parser.add_argument('--mode', choices=['local', 'glue'], default='local')
    parser.add_argument('--jars')
    args, unknown = parser.parse_known_args()
    if args.mode == 'local' and unknown:
        parser.error('Unrecognized local arguments: ' + ' '.join(unknown))
    # Glue injects its own JOB_NAME/etc arguments. No awsglue dependency in core code.
    spark = session(args.warehouse, args.catalog, args.mode, args.jars)
    try:
        print(json.dumps(run(spark, args.input, args.source, args.catalog, args.database)))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
