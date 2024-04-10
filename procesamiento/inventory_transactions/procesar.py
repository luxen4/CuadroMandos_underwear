from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql.functions import when, mean,  col, substring, lit, current_date, year, month, concat, to_date, date_format

# Inicializar la sesión de Spark
spark = SparkSession.builder \
    .appName("Sustitución de Nulos en Columna Específica") \
    .getOrCreate()


# Cargar el archivo CSV en un DataFrame de Spark
# df_filtrado = spark.read.csv("inventory_transactions.csv", header=True, inferSchema=True)
df_filtrado = spark.read.csv("./../../csv_originales/inventory_transactions.csv", header=True, inferSchema=True)


df_filtrado.show() 

"TransactionID","ProductID","PurchaseOrderID","MissingID","TransactionDate",
"UnitPurchasePrice","QuantityOrdered","QuantityReceived","QuantityMissing"
"1","1","","","5/29/2003",
"","28","28",""


# UnitPurchasePrice sería la media pero no traga 
listaSinDefinir=['UnitPurchasePrice','PurchaseOrderID','QuantityMissing']
for columna in listaSinDefinir :
    valor_reemplazo = 0
    df_filtrado = df_filtrado.na.fill({columna: valor_reemplazo})
    

listaSinDefinir=['MissingID']          # No hay csv con su ID 
for columna in listaSinDefinir :
    valor_reemplazo = 1
    df_filtrado = df_filtrado.na.fill({columna: valor_reemplazo})
    

# Sustituir los valores nulos en la columna de fecha con la fecha actual, la quito ya que es solo un registro y el gráfico sale mal.
columna_fecha = "TransactionDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})


# Sustituir los valores vacios con la media
listaMedia=['QuantityOrdered','QuantityReceived']
for columna in listaMedia :
    mean_quantity_sold = df_filtrado.select( mean(col(columna))).collect()[0][0]
    #mean_quantity_sold = round(mean_quantity_sold,2)
    df_filtrado = df_filtrado.withColumn(columna, when(col(columna).isNull(), mean_quantity_sold).otherwise(col(columna)))


# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "TransactionDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%m/%d/%Y")})        # Nulos a fecha actual
df_filtrado = df_filtrado.withColumn(columna_fecha, to_date(col(columna_fecha), "M/d/yyyy"))     # Castear a Date
df_filtrado = df_filtrado.withColumn("Año", year(columna_fecha))
df_filtrado = df_filtrado.withColumn("Mes", month(columna_fecha))
df_filtrado = df_filtrado.withColumn("Mes/Año", concat(month(columna_fecha), lit("/"), year(columna_fecha)))


# Eliminar registros con valores nulos en una columna específica,
# SOLO HAY UN PAR E REGISTROS, SE ELIMINAN, NO MERECE LA PENA SACAR EL GRAFICO EN BLANCO
columna_a_evaluar = 'TransactionDate'
df_filtrado = df_filtrado.dropna(subset=[columna_a_evaluar])


#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("inventory_transactionslimpia.csv", index=False)

# Detener la sesión de Spark
spark.stop()


#### Meter este código en Orders