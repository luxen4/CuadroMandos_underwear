from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import when, mean,  col, substring, lit, current_date,day, year, month, concat, to_date, date_format

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Sustitución de Nulos en Columna Específica") \
    .getOrCreate()

# df_filtrado = spark.read.csv("products.csv", header=True, inferSchema=True)
df_filtrado = spark.read.csv("./../../csv_originales/products.csv", header=True, inferSchema=True)


df_filtrado.show() 

listaSinDefinir=['ProductName', 'Color', 'ModelDescription', 'FabricDescription', 'Category', 'Gender', 'PackSize', 'ProductLine', 'Weight', 'Status']
for columna in listaSinDefinir :
    valor_reemplazo = "Sin definir"
    df_filtrado = df_filtrado.na.fill({columna: valor_reemplazo})
    

# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "InventoryDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})        # Nulos a fecha actual
df_filtrado = df_filtrado.withColumn(columna_fecha, to_date(col(columna_fecha), "M/d/yyyy"))     # Castear a Date
df_filtrado = df_filtrado.withColumn("Año", year(columna_fecha))
df_filtrado = df_filtrado.withColumn("Mes", month(columna_fecha))
df_filtrado = df_filtrado.withColumn("Dia", day(columna_fecha))
df_filtrado = df_filtrado.withColumn("Mes/Año", concat(month(columna_fecha), lit("/"), year(columna_fecha)))





# Sustituir los valores vacios con la media
listaMedia=['FabricDescription','PurchasePrice', 'Weight', 'Size']
for columna in listaMedia :
    mean_quantity_sold = df_filtrado.select(mean(col(columna))).collect()[0][0]
    mean_quantity_sold = round(mean_quantity_sold,2)
    df_filtrado = df_filtrado.withColumn(columna, when(col(columna).isNull(), mean_quantity_sold).otherwise(col(columna)))


#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("productslimpia.csv", index=False)

# Detener la sesión de Spark
spark.stop()
