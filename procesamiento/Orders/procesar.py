from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import col, mean , year, month ,lit, concat, substring                # python3 -m pip install numpy
from pyspark.sql.functions import current_date, when, to_date

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Sustitución de Nulos en Columna Específica") \
    .getOrCreate()

df = spark.read.csv("ordersprueba.csv", header=True, inferSchema=True)
#df = spark.read.csv("./../../csv_originales/orders.csv", header=True, inferSchema=True)

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


# Fechas van a mes/dia/año
columna_fecha = "OrderDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})        # Nulos a fecha actual
df_filtrado = df_filtrado.withColumn(columna_fecha, to_date(col("OrderDate"), "M/d/yyyy"))     # Castear a Date
df_filtrado = df_filtrado.withColumn("Año", year("OrderDate"))
df_filtrado = df_filtrado.withColumn("Mes", month("OrderDate"))
df_filtrado = df_filtrado.withColumn("Mes/Año", concat(month("OrderDate"), lit("/"), year("OrderDate")))


columna_fecha = "ShipDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})

columna_especifica = "FreightCharge"
valor_reemplazo = 0.0
df_filtrado = df_filtrado.na.fill({columna_especifica: valor_reemplazo})

#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("orderslimpia.csv", index=False)

spark.stop()        # Detener la sesión de Spark