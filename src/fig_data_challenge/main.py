import argparse


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="subparser_name")
    parser_run = subparsers.add_parser("run")
    parser_run.add_argument(
        "mode",
        choices=[
            "db-migration",
            "start-etl",
        ],
    )

    args, unknownargs = parser.parse_known_args()

    if args.subparser_name == "run" and args.mode == "db-migration":
        from db_migration.clickhouse import start_db_migration

        start_db_migration()

    elif args.subparser_name == "run" and args.mode == "start-etl":
        from fig_data_challenge.etl import master_etl

        master_etl()


if __name__ == "__main__":
    main()
