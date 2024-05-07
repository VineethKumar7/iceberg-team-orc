import pyarrow as pa
from pyarrow import orc
import pyarrow.orc as orc
from datetime import datetime
import os

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
    for file in orc_files:
        min_date, max_date = find_min_max_date(file_path=file, column='l_shipdate')
        print(min_date, max_date)