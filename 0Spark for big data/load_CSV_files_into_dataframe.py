#tutorial from https://www.analyticsvidhya.com/blog/2016/10/spark-dataframe-and-operations/

#DataFrame
## Creating DataFrame from CSV files
## the package for loading csv file data into DataFrame
### https://github.com/databricks/spark-csv
#from pyspark.sql import SQLContext
#sqlContext = SQLContext(sc)
sqlContext = HiveContext(sc)
df = spark.read.csv('file:///scratch/gul15103/spark/csvData/train.csv',header='true', inferSchema='true') 

### see the datatypes of columns
df.printSchema()
###other functions
df.show()
df.count()
# print columns' name
df.columns

# describe operation is use to calculate the summary statistics of numerical column(s) in DataFrame. 
#If we don’t specify the name of columns it will calculate summary statistics for all numerical columns present in DataFrame.
df.describe().show()
df.describe('Product_ID').show()
# To select specific columns in dataframe
df.select('User_ID','Age').show(5)
#calculate frequence table for two catigorical variables
df.crosstab('Age', 'Gender').show()
#remove duplicates
df.select('Age','Gender').dropDuplicates().show()
#drop all rows with NA
df.dropna()

#Convert dataframe to sql database
df.registerTempTable( "df_table")
sqlContext.sql('select Product_ID from table1').show(5)

