import os
from functools import reduce
from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, size, format_string
from pyspark.sql.types import FloatType

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
i_time = time()
DATASET_PATH = 'hdfs:///BigDataCSV'  # 'hdfs://192.168.104.45:9000/user/amircoli/BDA2324'
OUTPUT_PATH = '/home/user/results/5'  # '/home/amircoli/BDAchallenge2324/results/5'
COLUMNS_OF_INTEREST = ['LATITUDE', 'LONGITUDE', 'WND', 'TMP']
COLUMNS_HAVE_SAME_INDEX = False

# Create the output path if it doesn't already exist
try:
    os.makedirs(OUTPUT_PATH)
except OSError as e:
    pass


def list_file_names(directory_path):
    """
    List all files in a given directory on the hdfs filesystem
    """
    file_status_objects = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration()).listStatus(
        sc._jvm.org.apache.hadoop.fs.Path(directory_path)
    )
    return sorted([str(file.getPath().getName()) for file in file_status_objects])


def write_to_file(filename, content):
    with open('{}/{}'.format(OUTPUT_PATH, filename), "w") as file:
        file.write(content)


def read_headers():
    """
    Read headers from the CSV files and return them as a dict, where the key is a tuple of the indices
    of the columns of interest, and the value is the list of file paths
    Note: To minimize the number of unionByName operations, we only look at the indexes of the columns
    of interest of the subsequent queries
    """
    headers = {}
    for year in list_file_names(DATASET_PATH):
        for station in list_file_names('{}/{}'.format(DATASET_PATH, year)):
            path = '{}/{}/{}'.format(DATASET_PATH, year, station)
            header = spark.read.option("header", "true").csv(path).columns
            # Another way to go is with smart_open:
            # header = smart_open.smart_open('{}/{}/{}'.format(dataset_path, year, station), 'r').readline().strip()
            key = tuple(header.index(c) for c in COLUMNS_OF_INTEREST)
            headers[key] = headers.get(key, []) + [(year, station)]
    return headers


def read_csv(csv_files=None):
    """
    Perform a spark read operation and use the _metadata column to create two columns,
    one for the year and one for the station name
    """
    if csv_files is None:
        csv_files = '{}/*/*.csv'.format(DATASET_PATH)
    return spark.read.format('csv') \
        .option('header', 'true') \
        .load(csv_files) \
        .withColumn('year', split(col('_metadata.file_path'), '/')) \
        .withColumn('year', col('year')[size('year') - 2]) \
        .withColumn('station', split(col('_metadata.file_name'), '.csv')[0])


def op1(df):
    """
    Op1: print out the number of measurements taken per year for each station (sorted by year and station)
    Note: this is fast as it exploits on spark dataframe transformations
    """
    result_df = df \
        .select(['year', 'station']) \
        .groupBy('year', 'station') \
        .agg(count('*').alias('num_measures')) \
        .orderBy('year', 'station') \
        .select(format_string("%s,%s,%d", "year", "station", "num_measures").alias('result'))
    # out_file_path = '{}/o1.txt'.format(output_path)
    # if os.path.exists(out_file_path):
    #     os.remove(out_file_path)
    # df.write.format("text").option("header", "false").mode("append").save(out_file_path)
    write_to_file('o1.txt', '\n'.join([row.result for row in result_df.rdd.collect()]))


def op2(df):
    """
    Op2: print the top 10 temperatures (TMP) with the highest number of occurrences and count recorded
    in the highlighted area (sorted by number of occurrences and temperature)
    """
    result_df = df \
        .withColumn("LATITUDE", col('LATITUDE').cast(FloatType())) \
        .withColumn("LONGITUDE", col('LONGITUDE').cast(FloatType())) \
        .select(['LATITUDE', 'LONGITUDE', 'TMP']) \
        .filter((col('LATITUDE') >= 30) & (col('LATITUDE') <= 60) &
                (col('LONGITUDE') >= -135) & (col('LONGITUDE') <= -90)) \
        .groupBy('TMP') \
        .agg(count('*').alias('num_occurrences')) \
        .orderBy(col("num_occurrences").desc(), col("TMP").asc())
    results = []
    for row in result_df.rdd.take(10):
        tmp = float(str(row.TMP).split(',')[0]) / 10
        results.append('[(60,-135);(30,-90)],{},{})'.format(tmp, row.num_occurrences))
    write_to_file('o2.txt', '\n'.join(results))


def op3(df):
    """
    Op3: print out the station with the speed in knots and its count
    (sorted by count, speed and station)
    """
    result_df = df \
        .select(['station', 'WND']) \
        .withColumn('WDN_knots', split(col('WND'), ',')[1]) \
        .groupBy('station', 'WDN_knots') \
        .agg(count('*').alias('num_occurrences')) \
        .orderBy(col("num_occurrences").desc(), col("WDN_knots").asc(), col("station").asc())
    first_row = result_df.first()
    write_to_file('o3.txt', '{},{},{}'.format(first_row.station, first_row.WDN_knots, first_row.num_occurrences))


if __name__ == '__main__':
    # Read all csv files
    if COLUMNS_HAVE_SAME_INDEX:
        union_df = read_csv()
    else:
        headers = read_headers()
        print('CSV headers checked in {} s.'.format(time() - i_time))
        dfs = []
        for stations in headers.values():
            files = ['{}/{}/{}'.format(DATASET_PATH, year, station) for year, station in stations]
            dfs.append(read_csv(files))
        # union_df = dfs[0]
        # for df in dfs[1:]:
        #     union_df = union_df.unionByName(df, allowMissingColumns=True)
        # Alternatively, with the `reduce` spark function
        union_df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dfs)
    # union_df.cache()

    print('Dataframe loaded (lazy) in {} s.'.format(time() - i_time))
    op1(union_df)
    print('Op 1 terminated in {} s.'.format(time() - i_time))
    op2(union_df)
    print('Op 2 terminated in {} s.'.format(time() - i_time))
    op3(union_df)
    print('All operations have terminated in {} s.'.format(time() - i_time))
