import time

import duckdb
import pyarrow as pa
from pyarrow import orc
import pyarrow.orc as orc
from datetime import datetime
import os
import pandas as pd

ware_house_path = 'warehouse/default/lineitem/data/'
for dirs, subdirs, files in os.walk(ware_house_path):
    pass

orc_files = [ware_house_path + str(file) for file in files]

def find_min_max_date(file_path: str, column: str='l_shipdate'):
    """This function returns the min and max values of the given column"""
    #@todo this information should be extracted properly from the meta data, this is a temp solution
    # Open the ORC file
    file = orc.ORCFile(file_path)
    cols = list(file.read(columns=[column])[0])
    cols = list(map(str, cols))

    dates = [datetime.strptime(date_str, '%Y-%m-%d') for date_str in cols]
    return min(dates), max(dates)

if __name__ == '__main__':
    query_results = []
    st_time = time.time()
    for i, file in enumerate(orc_files):
        min_date, max_date = find_min_max_date(file_path=file, column='l_shipdate')
        print(i, min_date, max_date)
        if max_date < pd.to_datetime("1994-01-01") or min_date > pd.to_datetime("1995-01-01"):
            pass
        else:
            df = pd.read_orc(file)

            lineitem_table = f"""
            CREATE TABLE IF NOT EXISTS lineitem{i} AS SELECT * FROM df
            """
            query6 = f"""
                    select
                      sum(l_extendedprice * l_discount) as revenue
                    from
                      lineitem{i}
                    where
                      l_shipdate >= date '1994-01-01'
                      and l_shipdate < date '1994-01-01' + interval '1' year
                      and l_discount between 0.06 - 0.01 and 0.06 + 0.01
                      and l_quantity < 24
                    """
            duckdb.query(lineitem_table)
            result = duckdb.query(query6)
            query_results.append(result.fetchone()[0])
    print(f"{query_results = }")
    print(f"{sum(query_results) = }")
    print(f"time taken for optimized query: {time.time() - st_time}")