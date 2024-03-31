from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
                 # python3 -m pip install numpy
from pyspark.sql.functions import  mean,  col, substring, lit, current_date, when

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Sustitución de Nulos en Columna Específica") \
    .getOrCreate()


# Cargar el archivo CSV en un DataFrame de Spark
df_filtrado = spark.read.csv("payments.csv", header=True, inferSchema=True)
# df_filtrado = spark.read.csv("payments - copia.csv", header=True, inferSchema=True)


df_filtrado.show() 
# "PaymentID","OrderID"," PaymentMethodID"," PaymentDate","PaymentAmount"
# "1","2","1","7/10/2003","603.50"


listaSinDefinir=['PaymentMethodID'] 
for columna in listaSinDefinir : 
    valor_reemplazo = 1 
    df_filtrado = df_filtrado.na.fill({columna: valor_reemplazo})
    

# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "PaymentDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m-%d-%Y")})


# Sustituir los valores vacios con la media
listaMedia=['PaymentAmount']
for columna in listaMedia :
    mean_quantity_sold = df_filtrado.select(mean(col(columna))).collect()[0][0]
    mean_quantity_sold = round(mean_quantity_sold,2)
    df_filtrado = df_filtrado.withColumn(columna, when(col(columna).isNull(), mean_quantity_sold).otherwise(col(columna)))


# Ojo que las fechas van a mes/dia/año
# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "PaymentDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})
df_filtrado = df_filtrado.withColumn("Año", substring(col("PaymentDate"), -4, 4).cast("int"))
df_filtrado = df_filtrado.withColumn("Mes", substring(col("PaymentDate"), -10, 2).cast("int"))


''' No traga
# Extract month as an integer
mes = substring(col("PaymentDate"), -10, 2).cast("int")

# Extract year
ano = substring(col("PaymentDate"), -4, 4)

# Concatenate month and year
mesAno = mes.astype('string') + '/' + ano
print(mesAno)
# Add the new column to the DataFrame
df_filtrado = df_filtrado.withColumn("Mes/Año", lit(mesAno))
'''


#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("paymentslimpiaa.csv", index=False)

# Detener la sesión de Spark
spark.stop()


#### Meter este código en Orders