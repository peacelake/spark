# spark-submit /users/shangj/Documents/github/python/test.py
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)


lines = sc.textFile("README.md")
lines.count()

def hasPython(line):
	return "Python" in line

pythonLines = lines.filter(hasPython) 
pythonLines.count()
pythonLines.first()
