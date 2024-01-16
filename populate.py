# Copyright (c) 2024 Linh Pham
# wwdtm_populate_show_urls is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Show URL Populator Script."""
import csv
import json
import sys
from argparse import ArgumentParser, Namespace
from datetime import date
from pathlib import Path

from mysql.connector import connect as mysql_connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


def parse_command() -> Namespace:
    """Parse command-line argument, options and flags."""
    parser: ArgumentParser = ArgumentParser(
        description="Populates NPR.org show URLs in the Wait Wait Stats Database."
    )
    parser.add_argument(
        "-f",
        "--file",
        help="CSV file containing show date in YYYY-MM-DD format and corresponding NPR.org URL",
        type=str,
    ),
    parser.add_argument(
        "-b",
        "--backfill",
        action="store_true",
        help="Populate show URLs with generated links after ingesting CSV file",
    )

    return parser.parse_args()


def parse_database_config(
    config_file: str = "config.json",
) -> dict[str, str | int | bool] | None:
    """Parse configuration JSON file."""
    config_file_path: Path = Path.cwd() / config_file
    with config_file_path.open(mode="r", encoding="utf-8") as config:
        connect_config = json.load(config)

    if not connect_config or "database" not in connect_config:
        return None

    database_config: dict[str, str | int | bool] = connect_config["database"]
    if "autocommit" not in database_config or not database_config["autocommit"]:
        database_config["autocommit"] = True

    return database_config


def read_csv(file_name: str) -> dict[str, str]:
    """Returns a dictionary containing the contents of a show URLs CSV file."""
    csv_file: Path = Path(file_name)
    shows: dict = {}
    with csv_file.open(mode="r", encoding="utf-8") as show_urls_file:
        reader = csv.DictReader(show_urls_file)

        for line in reader:
            shows[line["date"]] = line["url"]

    return shows


def npr_show_url(show_date: date) -> str:
    """Generates an NPR.org Show Page URL from a date."""
    url_prefix = "https://www.npr.org/programs/wait-wait-dont-tell-me/archive?date="
    legacy_url_prefix = "https://legacy.npr.org/programs/waitwait/archrndwn"
    legacy_url_suffix = ".waitwait.html"

    if show_date >= date(year=2006, month=1, day=7):
        show_date_string = show_date.strftime("%m-%d-%Y")
        return f"{url_prefix}{show_date_string}"
    else:
        show_date_string: str = show_date.strftime("%y%m%d")
        year: str = show_date.strftime("%Y")
        month: str = show_date.strftime("%b").lower()
        return (
            f"{legacy_url_prefix}/{year}/{month}/{show_date_string}{legacy_url_suffix}"
        )


def retrieve_show_dates(
    database_connection: MySQLConnection | PooledMySQLConnection,
) -> list[str] | None:
    """Returns a list of show dates."""
    cursor = database_connection.cursor(named_tuple=True)
    query = """
        SELECT showdate FROM ww_shows
        ORDER By showdate ASC;
    """
    cursor.execute(query)
    shows = cursor.fetchall()
    cursor.close()

    if not shows:
        return None

    return [show.showdate.isoformat() for show in shows]


def update_urls_from_dict(
    show_urls: dict[str, str],
    database_connection: MySQLConnection | PooledMySQLConnection,
) -> None:
    """Update show URL values from a dictionary.

    The dictionary should contain a show date as the key and the NPR.org
    show URL as the value.
    """
    if not show_urls:
        print("ERROR: No show URLs provided.")
        return None

    all_show_dates = retrieve_show_dates(database_connection=database_connection)

    for show in show_urls:
        if show not in all_show_dates:
            print(f"INFO: Show date {show} is not found. Skipping.")
            continue

        cursor = database_connection.cursor()
        query = """
            UPDATE ww_shows SET showurl = %s
            WHERE showdate = %s;
        """
        cursor.execute(
            query,
            (
                show_urls[show],
                show,
            ),
        )

    cursor.close()
    return None


def update_urls_using_template(
    database_connection: MySQLConnection | PooledMySQLConnection,
) -> None:
    """Update show database returns with generated URLs if NULL."""
    cursor = database_connection.cursor(named_tuple=True)
    query = """
        SELECT showdate FROM ww_shows
        WHERE showurl IS NULL
        ORDER BY showdate ASC;
    """
    cursor.execute(query)
    shows = cursor.fetchall()
    cursor.close()

    if not shows:
        print("INFO: No show URLs to update.")
        return None

    for show in shows:
        show_url: str = npr_show_url(show_date=show.showdate)
        cursor = database_connection.cursor()
        query = """
            UPDATE ww_shows SET showurl = %s
            WHERE showdate = %s
        """
        cursor.execute(query, (show_url, show.showdate))

    cursor.close()


def main() -> None:
    """Application entry."""
    command_args: Namespace = parse_command()
    database_config = parse_database_config(config_file="config.json")

    if not database_config:
        print("ERROR: Database configuration file is not valid.")
        sys.exit(1)

    shows = read_csv(file_name=command_args.file)
    if not shows:
        print("INFO: No shows found in CSV file. Skipping.")

    database_connection = mysql_connect(**database_config)

    update_urls_from_dict(show_urls=shows, database_connection=database_connection)

    if command_args.backfill:
        update_urls_using_template(database_connection=database_connection)

    return None


if __name__ == "__main__":
    main()
