library(reticulate)

# virtualenv_create("sparklyr2")
# install_python()

# virtualenv_install(
#   envname = "sparklyr2",
#   packages = c("pyspark", "pandas", "PyArrow", "grpcio",
#                "google-api-python-client", "grpcio_status"
#   )
# )

library(reticulate)

use_python("~/.pyenv/versions/3.9.13/bin/python3.9")
use_virtualenv("sparklyr2")


pyspark <- import("pyspark")
pyspark_sql <- pyspark$sql
remote <- pyspark_sql$SparkSession$builder$remote("sc://localhost")
spark <- remote$getOrCreate()

df_python <- r_to_py(mtcars)
df_spark <- spark$createDataFrame(df_python)
df_cached <- df_spark$cache()
df_view <- df_cached$createTempView("spark_mtcars")
sql_res <- spark$sql("Select * from spark_mtcars")
sql_res$toPandas()

library(dplyr)
library(dbplyr)

con <- simulate_hive()
simulated_table <- tbl_lazy(mtcars, con, name = "spark_mtcars")

dplyr_test <-simulated_table %>%
  group_by(am) %>%
  summarise(mean_mpg = mean(mpg, na.rm = TRUE))

remote_query(dplyr_test)

run_sql <- dplyr_test %>%
  remote_query() %>%
  spark$sql()

run_sql$toPandas()

#spark$client$close()


library(reticulate)
library(dplyr)
devtools::load_all()
use_python("~/.pyenv/versions/3.9.13/bin/python3.9")
use_virtualenv("sparklyr2")

sc <- spark_connect(host = "sc://localhost")

tbl_iris <- copy_to(sc, iris)

sc$sql("select * from iris")
