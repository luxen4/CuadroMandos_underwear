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
df = spark.read.csv("orders.csv", header=True, inferSchema=True)
df = spark.read.csv("orders - copia.csv", header=True, inferSchema=True)

# Sustituir los valores nulos en la columna específica "OrderID" con un valor específico (por ejemplo, 0)
columna_especifica = "OrderID"
valor_reemplazo = 0
df_filtrado = df.na.fill({columna_especifica: valor_reemplazo})

columna_especifica = "CustomerID"
valor_reemplazo = 0
df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

columna_especifica = "EmployeeID"
valor_reemplazo = 0
df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

columna_especifica = "ShippingMethodID"
valor_reemplazo = 9
df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})


# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "OrderDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%d-%m-%Y")})

columna_fecha = "ShipDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%d-%m-%Y")})

columna_especifica = "FreightCharge"
valor_reemplazo = 0.0
df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

'''
# Sustituir los valores vacios con la media
columna_especifica = "quantity_sold"
mean_quantity_sold = df_filtrado.select(mean(col(columna_especifica))).collect()[0][0]
mean_quantity_sold = round(mean_quantity_sold,2)
df_filtrado = df_filtrado.withColumn(columna_especifica, when(col(columna_especifica).isNull(), mean_quantity_sold).otherwise(col(columna_especifica)))

'''

#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos

df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("orderslimpia.csv", index=False)

# Detener la sesión de Spark
spark.stop()