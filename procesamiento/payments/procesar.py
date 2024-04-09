from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import  mean,  col, substring, lit, current_date, when, year, month, concat, to_date, date_format

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Sustitución de Nulos en Columna Específica") \
    .getOrCreate()

# Cargar el archivo CSV en un DataFrame de Spark
# df_filtrado = spark.read.csv("payments.csv", header=True, inferSchema=True)
df_filtrado = spark.read.csv("./../../csv_originales/payments.csv", header=True, inferSchema=True)


df_filtrado.show() 
# "PaymentID","OrderID"," PaymentMethodID"," PaymentDate","PaymentAmount"
# "1","2","1","7/10/2003","603.50"


listaSinDefinir=['PaymentMethodID'] 
for columna in listaSinDefinir : 
    valor_reemplazo = 1 
    df_filtrado = df_filtrado.na.fill({columna: valor_reemplazo})
    

# Sustituir los valores vacios con la media
listaMedia=['PaymentAmount']
for columna in listaMedia :
    mean_quantity_sold = df_filtrado.select(mean(col(columna))).collect()[0][0]
    mean_quantity_sold = round(mean_quantity_sold,2)
    df_filtrado = df_filtrado.withColumn(columna, when(col(columna).isNull(), mean_quantity_sold).otherwise(col(columna)))


# Fechas van a mes/dia/año
columna_fecha = "PaymentDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})        # Nulos a fecha actual
df_filtrado = df_filtrado.withColumn(columna_fecha, to_date(col("PaymentDate"), "M/d/yyyy"))     # Castear a Date
df_filtrado = df_filtrado.withColumn("Año", year("PaymentDate"))
df_filtrado = df_filtrado.withColumn("Mes", month("PaymentDate"))
df_filtrado = df_filtrado.withColumn("Mes/Año", concat(month("PaymentDate"), lit("/"), year("PaymentDate")))


#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("paymentslimpia.csv", index=False)

# Detener la sesión de Spark
spark.stop()

