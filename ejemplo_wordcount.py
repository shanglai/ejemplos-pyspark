# Databricks notebook source
# Esta funcion elimina la puntuación del texto que recibe como argumento
def removePunctuation(text):
  import re
  return re.sub(r'[^A-Za-z0-9 ]', '', text).lower().strip()

fileName = '/databricks-datasets/cs100/lab1/data-001/shakespeare.txt'

# 1. Cargue el archivo localizado en fileName como un RDD llamado shakespeareRDD.
shakespeareRDD = sc.textFile(fileName)


# COMMAND ----------

print(removePunctuation('Ejemplo, para todos, de la función de quitar puntuación 2017. Obvio, no se tomaron en cuenta los acentos...'))

# COMMAND ----------



# COMMAND ----------

shakespeareRDD.count()

# COMMAND ----------

shakespeareRDD.take(10)

# COMMAND ----------


# 2. Aplique la función removePunctuation al contenido de shakespeareRDD. Use map.
shakespeareRDD = shakespeareRDD.map(lambda x:removePunctuation(x))
shakespeareRDD.take(10)


# COMMAND ----------

# 3. Transforme shakespeareRDD a shakespeareWordsRDD separando las palabras por espacios. Use map con la función split(' ')
shakespeareWordsRDD = shakespeareRDD.flatMap(lambda x:x.split(' '))
shakespeareWordsRDD.take(10)


# COMMAND ----------


# 4. Elimine las palabras de longitud 0 de shakespeareWordsRDD. Guarde el resultado en wordsRDD. Use filter.
wordsRDD = shakespeareWordsRDD.filter(lambda x:len(x) > 0)
wordsRDD.take(10)


# COMMAND ----------

# 5. Imprima el conteo de palabras en wordsRDD. Use count
print(wordsRDD.count())

# COMMAND ----------

# 6. Calcule la frecuencia de cada palabra en wordsRDD y guarde el resultado en freqRDD. Use map y reduceByKey.
freqRDD = wordsRDD.map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)
freqRDD.take(10)


# COMMAND ----------

# 7. Imprima el contenido de freqRDD. Use collect y display
display(freqRDD.collect())
