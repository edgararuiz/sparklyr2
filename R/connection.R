#' Connects to a Spark environment
#' @param host The URL or path for the master, or the remote, parameter
#' @export
spark_connect <- function(host) {
  pyspark <- import("pyspark")
  pyspark_sql <- pyspark$sql
  # Infers connection type by reading the value of host
  if(grepl("sc://", host)) {
    remote <- pyspark_sql$SparkSession$builder$remote(host)
    con_class <- "sparklyr2_connect"
  }
  out <- remote$getOrCreate()
  class(out) <- c(class(out), "sparklyr2_connection", con_class)
  out
}


