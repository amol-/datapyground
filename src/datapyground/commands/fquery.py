"""Command line interface for executing SQL queries on files.

This module provides a command line interface for executing SQL queries on files
based on the DataPyground :class:`datapyground.sql.SQLParser` and
:class:`datapyground.sql.SQLQueryPlanner`.

The results of the execution are then printed to the console in a tabular format
using the :class:`datapyground.utils.tabulate` module.
"""

import argparse

import pyarrow as pa

from datapyground.sql import Parser, SQLExpressionError, SQLParseError, SQLQueryPlanner
from datapyground.utils import tabulate


def main() -> None:
    """Parse the command line arguments and execute the SQL query."""
    parser = argparse.ArgumentParser(description="Process some SQL query on files.")
    parser.add_argument(
        "-t",
        "--table",
        action="append",
        help="Map a table name to a filepath. Can be provided multiple times.",
    )
    parser.add_argument("query", type=str, help="The SQL query to execute.")
    args = parser.parse_args()

    catalog = {}
    if args.table:
        for table in args.table:
            table_name, file_path = table.split("=", 1)
            catalog[table_name] = file_path

    try:
        query = Parser(args.query).parse()
    except (SQLParseError, SQLExpressionError) as e:
        print(f"Invalid SQL, {e}")
        return

    plan = SQLQueryPlanner(query, catalog=catalog).plan()
    result = pa.Table.from_batches(plan.batches())
    print(tabulate.tabulate(result))


if __name__ == "__main__":
    main()
