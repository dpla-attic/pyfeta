# pyfeta

Python mapping examples.

Includes examples using the standard library `multiprocessing` module, and
using Apache Spark.

## Amazon Elastic MapReduce (EMR) setup

First, you need to make ready an S3 bucket that has the necessary read
permissions granted to the world, or to the EMR cluster, in order for it to
be able to read the `sparkconfig.json` file in the first command below.

When that's set, upload `cdl_map.py`, `emr-bootstrap.sh`, `spark_mapping.py`,
`spark_mapping_avro.py`, and `sparkconfig.json` to that bucket.

Modify the following example with the name of your S3 bucket and execute it.
You will also have to update the Security Group IDs if you're not running this
on the DPLA infrastructure.

```
aws emr create-cluster --applications Name=Spark \
    --ec2-attributes '{"KeyName":"general","InstanceProfile":"EMR_EC2_DefaultRole","ServiceAccessSecurityGroup":"sg-07459c7a","SubnetId":"subnet-90afd9ba","EmrManagedSlaveSecurityGroup":"sg-0a459c77","EmrManagedMasterSecurityGroup":"sg-08459c75"}' \
    --service-role EMR_DefaultRole --release-label emr-5.1.0 \
    --log-uri 's3n://dpla-markb-test/elasticmapreduce/' \
    --name 'Mark B Test' \
    --instance-groups '[{"InstanceCount": 1, "InstanceGroupType": "MASTER", "InstanceType": "m3.xlarge", "Name":"Master - 1"}, {"InstanceCount": 2, "InstanceGroupType": "CORE", "InstanceType": "m3.xlarge", "Name": "Core - 2"}]' \
    --region us-east-1 \
    --bootstrap-action Path=s3://dpla-markb-test/emr-bootstrap.sh,Args=[] \
    --configurations https://s3.amazonaws.com/dpla-markb-test/sparkconfig.json
```

As soon as that returns with the ID of the new cluster, you can create a
"step" to launch the mapping job.

To launch `spark_mapping.py`:
```
aws emr add-steps --cluster-id j-14ZJHDYCP2EQT \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--py-files,s3a://dpla-markb-test/cdl_map.py,s3a://dpla-markb-test/spark_mapping.py,s3a://dpla-mdpdb/cdl.seq,s3a://dpla-markb-test/cdl-mapped.seq],ActionOnFailure=CONTINUE
```

To launch `spark_mapping_avro.py`:
```
aws emr add-steps --cluster-id j-2N2II4CDM4KYL \
    --steps Type=spark,Name=TestJob,Args=[--deploy-mode,cluster,--master,yarn,--conf,spark.yarn.submit.waitAppCompletion=true,--packages,com.databricks:spark-avro_2.11:3.0.1,--py-files,s3a://dpla-markb-test/cdl_map.py,s3a://dpla-markb-test/spark_mapping_avro.py,-p,8,s3a://dpla-markb-test/cdl_original.avro,s3a://dpla-markb-test/cdl_mapped.avro],ActionOnFailure=TERMINATE_JOB_FLOW
```
... noting that the number of RDD partitions, given to `spark_mapping_avro.py`
by the option `-p 8`, matches the number of CPU cores that are available based
on the count of "CORE" instances you specified in the `create-cluster` command
above.  In the examples above, two m3.xlarge instances (with four cores each)
have eight CPUs available, so we partition the RDD eight ways. 

When the step is complete, you should have an Avro or SequenceFile folder inside
of your bucket with the mapped files.
