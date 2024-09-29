"""Format tabular data into a text table for print."""

from typing import Any

from pyarrow import RecordBatch


def tabulate(recordbatch: RecordBatch, max_rows: int = 20) -> str:
    """Format a RecordBatch into a text table.

    Will produce a string like::

        Product   | Quantity | Price | Total
        --------- | -------- | ----- | ------
        Videogame | 8        | 66.50 | 532.00
        Laptop    | 8        | 38.72 | 309.76
        Laptop    | 7        | 77.46 | 542.22
    """
    cols = recordbatch.column_names
    rows = [
        [format_value(row[c]) for c in cols]
        for row in recordbatch.slice(length=max_rows).to_pylist()
    ]

    colsizes = compute_max_colsize(cols, rows)
    header = [maketablerow(cols, colsizes=colsizes)]
    separator = [maketablerow(["-"] * len(cols), colsizes=colsizes, fillvalue="-")]
    textrows = [maketablerow(row, colsizes=colsizes) for row in rows]

    table = "\n".join(header + separator + textrows)
    if recordbatch.num_rows > max_rows:
        table += f"\n... and {recordbatch.num_rows - max_rows} more rows"
    return table


def compute_max_colsize(cols: list[str], rows: list[list[str]]) -> list[int]:
    """Compute the maximum size of each column in a table."""
    return [
        max([len(row[colidx]) for row in rows] + [len(cols[colidx])])
        for colidx, _ in enumerate(cols)
    ]


def maketablerow(cols: list[str], colsizes: list[int], fillvalue: str = " ") -> str:
    """Make a table row with the given column sizes."""
    return " | ".join(
        [col.ljust(colsizes[idx], fillvalue) for idx, col in enumerate(cols)]
    )


def format_value(v: Any) -> str:
    """Format a value to be printed in the table.

    This function will format floats to 2 decimal places,
    and truncate long strings.
    """
    if isinstance(v, float):
        return f"{v:.2f}"
    elif isinstance(v, bool):
        return "true" if v else "false"

    v = str(v)
    if len(v) > 30:
        v = v[:27] + "..."
    return v
