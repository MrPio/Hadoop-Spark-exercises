import sys
from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
i_time = time()
dataset_path = 'hdfs://192.168.104.45:9000/user/amircoli/BDA2324'
output_path = '/home/amircoli/BDAchallenge2324/results/6'


class WeatherStationRDD:
    """
    Holds information about a weather station dataset
    """

    def __init__(self, name, years):
        self.name = name
        self.years = years

    def get_dataframe(self, year):
        return spark.read.options(header='True', delimiter=',').csv(self.get_path(year))

    def get_path(self, year):
        return f'{sys.argv[1]}/{year}/{self.name}.csv'

    def __str__(self):
        return f'Station Name: {self.name}, Years: {self.years}'


def list_file_names(directory_path):
    """
    List all files in a given directory
    :param sc: the SparkContext
    :param directory_path: the directory path in which the files are
    :return: the list of file names in the directory
    """
    file_status_objects = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()).listStatus(
        sc._jvm.org.apache.hadoop.fs.Path(directory_path)
    )
    return sorted([str(file.getPath().getName()) for file in file_status_objects])


years = [int(y) for y in list_file_names(sys.argv[1])]
if len(sys.argv) < 2:
    print('please specify the root folder of your dataset')
    sys.exit(1)
weather_stations = []


def scan_directory():
    """
    Scan the directory and store the csv information inside weather_stations objects
    :return: None
    """
    for year in years:
        for file in list_file_names(f'{sys.argv[1]}/{year}/'):
            filename = file.split('.')[0]
            found = False
            for ws in weather_stations:
                if ws.name == filename:
                    ws.years.append(year)
                    found = True
                    break
            if not found:
                weather_stations.append(WeatherStationRDD(filename, [year]))


def op1_slow():
    """
    Op1: print out the number of measurements taken per year for each station (sorted by year and station)
    Note: this is elegant but slow as it separately invokes spark for each file without exploiting its capabilities
    """
    scan_directory()
    results = []
    for year in years:
        for ws in weather_stations:
            if year in ws.years:
                results.append(f'{year},{ws.name},{ws.get_dataframe(year).count()}')
    print('\n'.join(results))


if __name__ == "__main__":
    op1_slow()
    print(f'Done with PySpark in {time() - i_time} s.')
