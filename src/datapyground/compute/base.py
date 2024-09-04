import abc
from typing import Iterator

import pyarrow as pa


class QueryPlanNode(abc.ABC):
    @abc.abstractmethod
    def batches(self) -> Iterator[pa.RecordBatch]:
        ...

    @abc.abstractmethod
    def __str__(self) -> str:
        ...
    

class Expression(abc.ABC):
    @abc.abstractmethod
    def apply(self, batch: pa.RecordBatch) -> pa.Array:
        ...

    @abc.abstractmethod
    def __str__(self) -> str:
        ...


class ColumnRef(Expression):
    def __init__(self, name) -> None:
        self.name = name

    def apply(self, batch: pa.RecordBatch) -> pa.Array:
        return batch.column(self.name)
    
    def __str__(self) -> str:
        return f"ColumnRef({self.name})"
col = ColumnRef