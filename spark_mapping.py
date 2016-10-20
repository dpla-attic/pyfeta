import sys
from pyspark import *
from cdl_map import CdlMap
from io import StringIO

o
def process(record):
    try:
        return record[0], CdlMap.map(StringIO(record[1].decode("utf8"))).decode("utf8")

    except:
        e = sys.exc_info()[0]
        return record[0], str(e)


if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: spark_mapping <MASTER> <IN> <OUT>", file=sys.stderr)
        exit(-1)

    conf = SparkConf().setAppName("CDL Mapping").setMaster(sys.argv[1])
    sc = SparkContext(conf=conf)

    input_data = sc.sequenceFile(sys.argv[2])
    output_data = input_data.map(lambda x: process(x))
    output_data.saveAsSequenceFile(sys.argv[3])
