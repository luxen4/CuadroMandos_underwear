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
df_filtrado = spark.read.csv("purchase_orders.csv", header=True, inferSchema=True)
# df_filtrado = spark.read.csv("products - copia.csv", header=True, inferSchema=True)

df_filtrado.show() 

"PurchaseOrderID","SupplierID","EmployeeID","ShippingMethodID","OrderDate"
"25","1","1","3","10/15/2003"

listaSinDefinir=['employeeID','ShippingMethodID']
for columna in listaSinDefinir :
    valor_reemplazo = 1
    df_filtrado = df_filtrado.na.fill({columna: valor_reemplazo})

# Sustituir los valores nulos en la columna de fecha con la fecha actual
columna_fecha = "OrderDate"
df_filtrado = df_filtrado.na.fill({columna_fecha: datetime.now().strftime("%d/%m/%Y")})


#df_filtrado.show()                                             # Mostrar el DataFrame con los valores nulos sustituidos
df_pandas = df_filtrado.toPandas()                              # Convertir el DataFrame de Spark a un DataFrame de Pandas
df_pandas.to_csv("purchase_orderslimpia.csv", index=False)

# Detener la sesión de Spark
spark.stop()


#### Meter este código en Orders