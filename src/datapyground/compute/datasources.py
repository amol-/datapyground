from typing import Iterator

import pyarrow as pa
import pyarrow.csv

from .base import QueryPlanNode


class CSVDataSource(QueryPlanNode):
    def __init__(self, filename) -> None:
        self.filename = filename

    def __str__(self) -> str:
        return f"CSVDataSource({self.filename})"
    
    def batches(self) -> Iterator[pa.RecordBatch]:
        with pa.csv.open_csv(self.filename) as reader:
            for batch in reader:
                yield batch