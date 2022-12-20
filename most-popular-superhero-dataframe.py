# Most Popular SuperHero

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///home/ko/workspace/SparkCourse/Marvel-names.txt")

# Marvel-Graph.txt:- This dataset consists of the superheroes' ids wherein the first id represents the superhero id, and the rest of the line represents the superheroes with whom it is friends.
lines = spark.read.text("file:///home/ko/workspace/SparkCourse/Marvel-graph.txt")

# split off hero ID from beginning of line
# Count how many space-separated numbers are in the line
# Subtract one to get the total number of connections
# Group by hero ID’s to add up connections split into multiple lines
connections = lines.withColumn("id", func.split(func.col("value"), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.col("value"), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# Sort by total connections
mostPopular = connections.sort(func.col("connections").desc()).first()

# Filter name lookup dataset by the most popular hero ID to look up the name
mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

