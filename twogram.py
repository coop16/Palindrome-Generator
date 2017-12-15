from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col, regexp_replace
import re

#Commandlines to run in EMR
#aws s3 cp s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data ./
#aws s3 cp s3://palindromengram/scripts/twogram.py ./
#spark-submit twogram.py

# Create a SparkSession 
spark = SparkSession.builder.appName("PalindromeQuery").getOrCreate()

#Mapper to convert to row object
#Note: remove all characters besides letters and spaces and make lowercase
def mapper(line):
    fields= line.split("\t") 
    return Row(ngram=str(re.sub("[^A-z ]", '',fields[0]).encode("utf-8").lower()), year=int(fields[1]), occur=int(fields[2]))

#Access data and convert into row object format
lines = spark.sparkContext.sequenceFile("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data")
stringlines=lines.map(lambda x: x[1])
gramsdf = stringlines.map(mapper)

#Filter by year (1990-2000)
filtered=gramsdf.filter( lambda x: x['year']>=1990 and x['year']<2000)

#Create data frame and table to query
df= spark.createDataFrame(filtered).cache()
df.createOrReplaceTempView("df")

#Query based on if palindrome, and aggregate to find sum of occurrences
#Note: ignore 2grams with length 2 and order by occurrences
palindromes = spark.sql("SELECT ngram, SUM(occur) FROM df WHERE LENGTH(REGEXP_REPLACE(ngram,' ',''))>2 AND REGEXP_REPLACE(ngram,' ','')==REVERSE(REGEXP_REPLACE(ngram,' ','')) GROUP BY ngram ORDER BY SUM(occur) DESC")


#Save file to single csv file in desired output bucket
palindromes.coalesce(1).write.csv("s3://palindromengram/output/twogram.csv")

spark.stop()
