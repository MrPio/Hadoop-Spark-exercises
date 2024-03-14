import os
from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, size, format_string
from pyspark.sql.types import FloatType

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
i_time = time()
dataset_path = 'hdfs://192.168.104.45:9000/user/amircoli/BDA2324'
output_path ='/home/amircoli/BDAchallenge2324/results/5'
# Create the output path if it does not already exist
try:
    os.makedirs(output_path)
except OSError as e:
    pass


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
        return '{}/{}/{}.csv'.format(dataset_path, year, self.name)

    def __str__(self):
        return 'Station Name: {}, Years: {}'.format(self.name, self.years)


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


years = []
weather_stations = []


def write_to_file(filename, content):
    with open('{}/{}'.format(output_path, filename), "w") as file:
        file.write(content)


def scan_directory():
    """
    Scan the directory and store the csv information inside weather_stations objects
    :return: None
    """
    for year in years:
        for file in list_file_names('{}/{}/'.format(dataset_path, year)):
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
    global years
    years = [int(y) for y in list_file_names(dataset_path)]
    scan_directory()
    results = []
    for year in years:
        for ws in weather_stations:
            if year in ws.years:
                results.append('{},{},{}'.format(year, ws.name, ws.get_dataframe(year).count()))
    write_to_file('o1.txt', '\n'.join(results))


def read_all_datasets():
    return spark.read.format('csv') \
        .option('header', 'true') \
        .load('{}/*/*.csv'.format(dataset_path)) \
        .withColumn('year', split(col('_metadata.file_path'), '/')) \
        .withColumn('year', col('year')[size('year') - 2]) \
        .withColumn('station', split(col('_metadata.file_name'), '.csv')[0])


def op1():
    """
    Op1: print out the number of measurements taken per year for each station (sorted by year and station)
    Note: this is fast as it exploits on spark dataframe transformations
    """
    df = read_all_datasets() \
        .select(['_metadata.file_path', 'year', 'station']) \
        .groupBy('year', 'station') \
        .agg(count('*').alias('num_measures')) \
        .orderBy('year', 'station') \
        .select(format_string("%s,%s,%d", "year", "station", "num_measures").alias('result'))
    # out_file_path = '{}/o1.txt'.format(output_path)
    # if os.path.exists(out_file_path):
    #     os.remove(out_file_path)
    # df.write.format("text").option("header", "false").mode("append").save(out_file_path)
    write_to_file('o1.txt', '\n'.join([row.result for row in df.rdd.collect()]))


def op2():
    """
    Op2: print the top 10 temperatures (TMP) with the highest number of occurrences and count recorded
    in the highlighted area (sorted by number of occurrences and temperature)
    """
    df = read_all_datasets() \
        .withColumn("LATITUDE", col('LATITUDE').cast(FloatType())) \
        .withColumn("LONGITUDE", col('LONGITUDE').cast(FloatType())) \
        .select(['LATITUDE', 'LONGITUDE', 'TMP']) \
        .filter((col('LATITUDE') >= 30) & (col('LATITUDE') <= 60) &
                (col('LONGITUDE') >= -135) & (col('LONGITUDE') <= -90)) \
        .groupBy('TMP') \
        .agg(count('*').alias('num_occurrences')) \
        .orderBy(col("num_occurrences").desc(), col("TMP").asc())
    results = []
    for row in df.rdd.take(10):
        tmp = float(str(row.TMP).split(',')[0]) / 10
        results.append('[(60,-135);(30,-90)],{},{})'.format(tmp, row.num_occurrences))
    write_to_file('o2.txt', '\n'.join(results))


def op3():
    """
    Op3: print out the station with the speed in knots and its count
    (sorted by count, speed and station)
    """
    df = read_all_datasets() \
        .select(['station', 'WND']) \
        .withColumn('WDN_knots', split(col('WND'), ',')[1]) \
        .groupBy('station', 'WDN_knots') \
        .agg(count('*').alias('num_occurrences')) \
        .orderBy(col("num_occurrences").desc(), col("WDN_knots").asc(), col("station").asc())
    first_row = df.first()
    write_to_file('o3.txt', '{},{},{}'.format(first_row.station, first_row.WDN_knots, first_row.num_occurrences))


if __name__ == '__main__':
    op1()
    op2()
    op3()
    print 'Operations terminated in {} s.'.format(time() - i_time)
