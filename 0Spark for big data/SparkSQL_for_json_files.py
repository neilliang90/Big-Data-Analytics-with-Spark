# Import Spark SQL
from pyspark.sql import HiveContext, Row
# Constructing a SQL context in Python
hiveCtx = HiveContext(sc)

inputFile = 'file:///scratch/gul15103/spark/jsonData/testweet.json'
input = spark.read.json(inputFile)
# Register the input schema RDD input.registerTempTable("tweets")
# Select tweets based on the retweetCount
topTweets = hiveCtx.sql("""SELECT text, retweetCount FROM
  tweets ORDER BY retweetCount LIMIT 10""")