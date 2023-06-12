#' Connects to a Spark environment
#' @param host The URL or path for the master, or the remote, parameter
#' @export
spark_connect <- function(host) {
  master <- ""
  remote <- ""
  # Infers connection type by reading the value of host
  if(grepl("sc://", host)) {
    pyspark <- import("pyspark")
    pyspark_sql <- pyspark$sql
    remote <- pyspark_sql$SparkSession$builder$remote(host)
    python <- remote$getOrCreate()
    con_class <- "sparklyr2_connect"
    remote <- host
  }

  structure(
    list(
      master = master,
      remote = remote,
      python = python,
      con = simulate_hive()
    ),
    class = c(con_class, "sparklyr2_connection", "DBIConnection")
  )
}

#' @rdname spark_connect
#' @export
spark_disconnect <- function(sc) {
  sc$python$client$close()
}

