import os
from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, count, size, lit
from pyspark.sql.types import FloatType

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
i_time = time()

DATASET_PATH = 'hdfs://192.168.104.45:9000/user/amircoli/BDA2324'
OUTPUT_PATH = '/home/amircoli/BDAchallenge2324/results/5'

COLUMNS_OF_INTEREST = ['LATITUDE', 'LONGITUDE', 'WND', 'TMP']
ASSUME_COLUMNS_OF_INTEREST_HAVE_SAME_INDEX = False

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


def write_to_file(filename, df):
    """
    Export a dataframe in the specified path as a CSV file
    """
    df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save('file://{}/{}'.format(OUTPUT_PATH, filename))


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
    Op1: print out the number of measurements taken per year
    for each station (sorted by year and station)
    """
    result_df = df \
        .select(['year', 'station']) \
        .groupBy('year', 'station') \
        .agg(count('*').alias('num_measures')) \
        .orderBy('year', 'station')
    write_to_file('op1', result_df)


def op2(df):
    """
    Op2: print the top 10 temperatures (TMP) with the highest number of occurrences and count recorded
    in the highlighted area (sorted by number of occurrences and temperature)
    """
    result_df = df \
        .withColumn("LATITUDE", col('LATITUDE').cast(FloatType())) \
        .withColumn("LONGITUDE", col('LONGITUDE').cast(FloatType())) \
        .withColumn("TMP", split(col('TMP'), ',')[0].cast(FloatType()) / 10) \
        .select(['LATITUDE', 'LONGITUDE', 'TMP']) \
        .filter((col('LATITUDE') >= 30) & (col('LATITUDE') <= 60) &
                (col('LONGITUDE') >= -135) & (col('LONGITUDE') <= -90)) \
        .groupBy('TMP') \
        .agg(count('*').alias('num_occurrences')) \
        .orderBy(col("num_occurrences").desc(), col("TMP").asc()) \
        .withColumn('Location', lit('[(60,-135);(30,-90)]')) \
        .select(['Location', 'TMP', 'num_occurrences']) \
        .limit(10)
    write_to_file('op2', result_df)


def op3(df):
    """
    Op3: print out the station with the speed in knots and its count
    (sorted by count, speed and station)
    """
    result_df = df \
        .select(['station', 'WND']) \
        .withColumn('WND', split(col('WND'), ',')[1]) \
        .groupBy('station', 'WND') \
        .agg(count('*').alias('num_occurrences')) \
        .orderBy(col("num_occurrences").desc(), 
                 col("WND").asc(), col("station").asc()) \
        .limit(1)
    write_to_file('op3', result_df)


if __name__ == '__main__':
    # Read all csv files
    if ASSUME_COLUMNS_OF_INTEREST_HAVE_SAME_INDEX:
        union_df = read_csv()
    else:
        headers = read_headers()
        dfs = []
        for stations in headers.values():
            files = ['{}/{}/{}'.format(DATASET_PATH, year, station) for year, station in stations]
            dfs.append(read_csv(files))
        union_df = dfs[0]
        for df in dfs[1:]:
            union_df = union_df.unionByName(df, allowMissingColumns=True)
        # Alternatively, with the `reduce` spark function:
        # union_df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dfs)

    op1(union_df)
    op2(union_df)
    op3(union_df)
    # print 'All operations have terminated in {} s.'.format(time() - i_time)
