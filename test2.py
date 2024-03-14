from time import time

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
i_time = time()


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


dataset_path = 'hdfs:///df_tmp2'
dfs = []
print(list_file_names(dataset_path))
# for file in list_file_names(dataset_path):
# dfs.append(spark.read.format('csv')
#           .option('header', 'true')
#           .load('{}/{}'.format(dataset_path, file))
#           .withColumn('station', split(col('_metadata.file_name'), '.csv')[0]))
# union_df = dfs[0]
# for df in dfs[1:]:
# 	union_df = union_df.unionByName(df, allowMissingColumns=True)
# union_df.show()
# 17 sec
#
# spark.read.format('csv') \
#           .option('header', 'true') \
#           .load('{}/*.csv'.format(dataset_path)) \
#           .withColumn('station', split(col('_metadata.file_name'), '.csv')[0]).show()
# 6 sec


print('Operations terminated in {} s.'.format(time() - i_time))
