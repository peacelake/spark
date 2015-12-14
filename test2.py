from pyspark import SparkConf, SparkContext

#conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1")
#sc = SparkContext("spark://127.0.0.1:7077", "test", conf)

host = "127.0.0.1"
keyspace = "gbs"
cf = "users"

sc = SparkContext(appName="CassandraInputFormat")
conf = {"cassandra.input.thrift.address": host,
            "cassandra.input.thrift.port": "9160",
            "cassandra.input.keyspace": keyspace,
            "cassandra.input.columnfamily": cf,
            "cassandra.input.partitioner.class": "Murmur3Partitioner",
            "cassandra.input.page.row.size": "3"}

cass_rdd = sc.newAPIHadoopRDD(
        "org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat",
        "java.util.Map",
        "java.util.Map",
        keyConverter="org.apache.spark.examples.pythonconverters.CassandraCQLKeyConverter",
        valueConverter="org.apache.spark.examples.pythonconverters.CassandraCQLValueConverter",
        conf=conf)

output = cass_rdd.collect()
for (k, v) in output:
	print((k, v))

sc.stop()
