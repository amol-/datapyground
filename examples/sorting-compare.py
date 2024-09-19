import sys
import time

import pandas
import psutil

from datapyground.compute import CSVDataSource, ExternalSortNode, SortNode

try:
    sorting_type = sys.argv[1]
except IndexError:
    sorting_type = None

if sorting_type == "external":
    # On development machine did lead to
    #   TIME: 84.4 MEMORY: 92
    q = ExternalSortNode(
        ["year"],
        [True],
        batch_size=10240,
        child=CSVDataSource("data/geounits.csv", block_size=1024 * 1024),
    )
elif sorting_type == "memory":
    # On development machine did lead to
    #   TIME: 2.0 MEMORY: 618
    q = SortNode(
        ["year"], [True], CSVDataSource("data/geounits.csv", block_size=1024 * 1024)
    )
elif sorting_type == "pandas":
    # On development machine did lead to
    #   TIME: 2.9 MEMORY: 409
    class FakeQuery:
        def batches(self):
            df = pandas.read_csv("data/geounits.csv")
            yield df.sort_values("year")

    q = FakeQuery()
else:
    print("Sorting must be external, memory or pandas")
    sys.exit(1)

proc = psutil.Process()
start = time.time()
for b in q.batches():
    continue
end = time.time()

print(
    "TIME:",
    round(end - start, 1),
    "MEMORY:",
    proc.memory_full_info().rss // (1024 * 1024),
)
