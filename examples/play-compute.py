import pyarrow.compute as pc

from datapyground.compute import FilterNode, CSVDataSource, col, FunctionCallExpression

query = FilterNode(
    FunctionCallExpression(pc.equal, col("City"), "Rome"),
    CSVDataSource("data/shops.csv")
)
for batch in query.batches():
    print("---")
    print(batch)