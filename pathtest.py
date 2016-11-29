"""
Test to verify the Python include paths and executable, running on Spark

Also demonstrates how you should send your program's output to an S3 object.
Amazon EMR's logging does not catch Python's stdout and stderr.

Usage:

spark-submit [options ...] pathtest.py <bucket name> <output file name>

Examples:

1) Local

spark-submit --master local pathtest.py YOUR_BUCKET_NAME pathtest.out

2) Amazon EMR

aws emr add-steps --cluster-id j-XXXXXXXX \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,\
--conf,spark.yarn.submit.waitAppCompletion=true,\
s3a://YOUR_BUCKET_NAME/pathtest.py,YOUR_BUCKET_NAME,pathtest.out],\
ActionOnFailure=CONTINUE

... where you need to fill in the ID of your own cluster and the name of your
S3 bucket.
"""

import sys
import boto3
from io import StringIO
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('Path Test')
sc = SparkContext(conf=conf)
out = StringIO()
s3 = boto3.resource('s3')
bucket = s3.Bucket(sys.argv[1])
fileobj = bucket.Object(sys.argv[2])

out.write("\n".join(sys.path) + "\n")
out.write(sys.version + "\n")
out.write(sys.executable + "\n")
fileobj.put(Body=out.getvalue())
