library(reticulate)

# virtualenv_create("sparklyr2")
# install_python()

use_python("~/.pyenv/versions/3.9.13/bin/python3.9")

virtualenv_install(
  envname = "sparklyr2",
  packages = c("pyspark", "pandas", "PyArrow", "grpcio",
               "google-api-python-client", "grpcio_status"
               )
  )

use_virtualenv("sparklyr2")

pyspark <- import("pyspark")
pyspark_sql <- pyspark$sql
remote <- pyspark_sql$SparkSession$builder$remote("sc://localhost")
spark <- remote$getOrCreate()

df_python <- r_to_py(mtcars)

df_spark <- spark$createDataFrame(df_python)

df_cached <- df_spark$cache()

df_view <- df_cached$createTempView("spark_mtcars")

spark$sql("Select * from spark_mtcars")

spark$client$close()
