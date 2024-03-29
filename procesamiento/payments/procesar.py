from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, mean                   # python3 -m pip install numpy
from pyspark.sql.functions import current_date, when

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
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%d-%m-%Y")})


# Sustituir los valores vacios con la media
listaMedia=['PaymentAmount']
for columna in listaMedia :
    mean_quantity_sold = df_filtrado.select(mean(col(columna))).collect()[0][0]
    mean_quantity_sold = round(mean_quantity_sold,2)
    df_filtrado = df_filtrado.withColumn(columna, when(col(columna).isNull(), mean_quantity_sold).otherwise(col(columna)))


#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("paymentslimpia.csv", index=False)

# Detener la sesión de Spark
spark.stop()


#### Meter este código en Orders