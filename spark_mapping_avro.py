r"""
Spark mapping with Avro files

Read Avro files as an alternative to the way SequenceFiles were used in
spark_mapping.py.

Invocation:

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

spark-submit \
    --master local[4] \
    --py-files cdl_map.py \
    --packages com.databricks:spark-avro_2.11:3.0.1 \
    spark_mapping_avro.py /path/to/original.avro /path/to/mapped.avro

For playing around with Avro in pyspark, do this after exporting
PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON as above:

    pyspark --packages com.databricks:spark-avro_2.11:3.0.1

See originalrecord.avsc for the schema of the Avro file. This demo assumes
that the Avro file you're loading adheres to this schema.
"""

import sys
import boto3
import traceback
from io import StringIO
from optparse import OptionParser
from cdl_map import CdlMap
try:
    import pyspark.sql
    from pyspark.sql.session import SparkSession
except ImportError:
    print("This module has to be run within Spark.\n"
          "For the sake of allowing this module to be run from the\n"
          "commandline for usage information, this warning is non-fatal.",
          file=sys.stderr)


DEFAULT_RDD_PARTITIONS = 4


def process(record):
    """Process a pyspark.sql.Row, return a new Row

    The Row is the row of the DataFrame that pyspark.rdd.RDD.map() iterates
    """
    try:
        return pyspark.sql.Row(body=CdlMap.map(StringIO(record.body))
                                          .decode('utf8'))
    except Exception as e:
        # In a real implementation I imagine there would be another field in
        # the Avro schema for storing errors or other processing messages, but
        # we'll just throw that in `body' for now:
        return pyspark.sql.Row(body=str(e))

def parse_cmdline(argv):
    """Parse commandline arguments, return tuple of (options, arguments)"""
    usage = "usage: %prog [options] <input Avro location> " \
            "<output Avro location> <log S3 bucket name>"
    epilog = "This module must be run within Spark, e.g. with spark-submit."
    parser = OptionParser(usage=usage, epilog=epilog)
    parser.add_option('-p', '--partitions', dest='rdd_partitions',
                      metavar='NUMBER', help='Number of RDD partitions',
                      default=DEFAULT_RDD_PARTITIONS)
    (options, args) = parser.parse_args(argv)  # TODO: validate / use argparse
    if len(args) != 3:
        parser.error('Input, output, and log bucket are required')
    return (options, args)

def main(argv):
    """Set up pyspark & logging resources, then read, map, and write data"""
    # We try to log output to S3, and we catch and log exceptions caught in
    # between setting up logging and PUTing the log object to S3.  If there's
    # an exception during those setup and PUT steps, we're out of luck, because
    # stderr from this program isn't going to be captured in EMR's stderr log.
    # We depend on boto3 and S3 working for us, and on having a writable bucket.
    (options, args) = parse_cmdline(argv)
    [input_uri, output_uri, log_bucket] = args
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(log_bucket)
    output_s3_obj = bucket.Object('spark_mapping_avro.log')
    prog_output = StringIO()
    rv = 0
    try:
        prog_output.write("Starting\n")
        spark = SparkSession.builder.appName('CDL Mapping').getOrCreate()
        # Avro files are managed as Spark DataFrames, but it's the RDD that has
        # the map() function that we need.  The RDD needs to be partitioned
        # into enough partitions to facilitate parallel processing.  The Avro
        # file could have just one partition, in which case it could only be
        # operated on by one Spark executor.
        df = spark.read.format('com.databricks.spark.avro').load(input_uri)
        new_rdd = df.rdd.repartition(int(options.rdd_partitions))  # assume num
        new_df = spark.createDataFrame(
                new_rdd.map(lambda record: process(record)))
        new_df.write.format('com.databricks.spark.avro').save(output_uri)
        prog_output.write("Done\n")
    except Exception as e:
        traceback.print_exc(limit=3, file=prog_output)
        rv = 1
    output_s3_obj.put(Body=prog_output.getvalue())
    return rv

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
