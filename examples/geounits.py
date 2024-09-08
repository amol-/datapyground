
def run_with_datapyground():
    import pyarrow.compute as pc

    from datapyground.compute import (
        CSVDataSource,
        FilterNode,
        FunctionCallExpression,
        PaginateNode,
        col,
    )

    query = PaginateNode(
        offset=0, length=5,
        child=FilterNode(
            FunctionCallExpression(pc.equal, col("year"), 2023),
            CSVDataSource("data/geounits.csv")
        )
    )
    for batch in query.batches():
        print(batch)


def run_with_pandas():
    import pandas as pd

    df = pd.read_csv("data/geounits.csv")
    df = df[df["year"] == 2023]
    print(df.head(5))


def download_and_extract_zip(url, extract_to, new_csv_name):
    import os
    import urllib.request
    import zipfile

    new_csv_path = os.path.join(extract_to, new_csv_name)
    if os.path.exists(new_csv_path):
        return

    zip_path = os.path.join(extract_to, 'file.zip')
    if not os.path.exists(extract_to):
        os.makedirs(extract_to)
    
    urllib.request.urlretrieve(url, zip_path)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    for file_name in os.listdir(extract_to):
        if file_name.endswith(".csv"):
            old_csv_path = os.path.join(extract_to, file_name)
            new_csv_path = os.path.join(extract_to, new_csv_name)
            os.rename(old_csv_path, new_csv_path)
    
    os.remove(zip_path)


if __name__ == "__main__":
    download_and_extract_zip(
        "https://www.stats.govt.nz/assets/Uploads/New-Zealand-business-demography-statistics/New-Zealand-business-demography-statistics-At-February-2023/Download-data/geographic-units-by-industry-and-statistical-area-20002023-descending-order-.zip",
        "data",
        "geounits.csv"
    )

    import timeit
    print("DataPyground Timing:", timeit.timeit(run_with_datapyground, number=1))
    print("")
    print("Pandas Timing:", timeit.timeit(run_with_pandas, number=1))
