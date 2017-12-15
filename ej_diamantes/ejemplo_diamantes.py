# Databricks notebook source
# Manipulación de Datos

# COMMAND ----------

## Lectura de archivo

# COMMAND ----------

diamonds = sqlContext.read.format('com.databricks.spark.csv').options(
header='true', inferSchema='true').load('/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv')

# COMMAND ----------

display(diamonds)

# COMMAND ----------

diamonds.printSchema()

# COMMAND ----------

print(diamonds.count())

# COMMAND ----------

display(diamonds.select("color").distinct().collect())

# COMMAND ----------

## Calculando los valores - precio / carat

# COMMAND ----------

from pyspark.sql.types import DoubleType
from pyspark.sql.functions import *
diamondsCast = diamonds.withColumn("price", diamonds["price"].cast(DoubleType()))
carat_avgPrice = (diamondsCast
               .groupBy("carat")
               .avg("price")
               .withColumnRenamed("avg(price)", "avgPrice")
               .orderBy(desc("avgPrice")))
carat_avgPrice.show(10)

# COMMAND ----------

### No sirve de mucho hacer un agrupado por una columna con valores continuos, así que podemos redondear un poco:
diamondsCast2= diamondsCast.withColumn("carat2",round(col("carat")))
diamondsCast2.show(10)

# COMMAND ----------

(diamondsCast2.groupBy("carat2").avg("price").withColumnRenamed("avg(price)", "avgPrice").orderBy(desc("avgPrice"))).show()

# COMMAND ----------

# También podríamos obtener el máximo de tabla por corte
from pyspark.sql.window import Window
import pyspark.sql.functions as func
windowSpec = Window.partitionBy(diamondsCast['cut']).orderBy(diamondsCast['table'].desc())
diamondsCast.select("cut","table",func.row_number().over(windowSpec).alias('lugar')).filter("lugar=1").show()

# COMMAND ----------

diamDf= diamondsCast.rdd
# Suma del precio por claridad
claridadPrecio= diamDf.map(lambda x: (x.clarity,x.price)).reduceByKey(lambda x,y:x+y)
display(claridadPrecio.collect())
