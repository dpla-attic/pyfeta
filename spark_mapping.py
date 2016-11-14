"""
A Python example that is made to be submitted with spark-submit

Example invocation, with a local Spark cluster on a machine with four cores:

spark-submit \
    --master local[4] \
    --py-files cdl_map.py \
    spark_mapping.py /path/to/sequencefile.seq /path/to/mapped_dir
"""

import sys
from pyspark import SparkConf, SparkContext
from cdl_map import CdlMap
from io import StringIO


def process(record):
    try:
        return record[0], CdlMap.map(StringIO(record[1].decode('utf8'))) \
                                .decode('utf8')

    except:
        e = sys.exc_info()[0]
        return record[0], str(e)


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: spark_mapping <IN> <OUT>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName('CDL Mapping')
    sc = SparkContext(conf=conf)

    input_data = sc.sequenceFile(sys.argv[1])
    output_data = input_data.map(lambda x: process(x))
    output_data.saveAsSequenceFile(
        sys.argv[2],
        'org.apache.hadoop.io.compress.DefaultCodec')
