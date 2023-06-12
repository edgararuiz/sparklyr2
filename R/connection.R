#' Connects to a Spark environment
#' @param host The URL or path for the master, or the remote, parameter
#' @export
spark_connect <- function(host) {
  master <- ""
  remote <- ""
  # Infers connection type by reading the value of host
  if (grepl("sc://", host)) {
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
    class = c("sparklyr2_connection", con_class, "DBIConnection")
  )
}

#' Disconnects from a Spark environment
#' @param sc Spark connection
#' @export
spark_disconnect <- function(sc) {
  sc$python$client$close()
}

#' @export
print.sparklyr2_connect <- function(x, ...) {
  cli_div(theme = cli_colors())
  cli_h3("{.val2 sparklyr2} - {.val1 Spark Connect} -")
  print_connection(x)
}

print_connection <- function(x) {
  cli_div(theme = cli_colors())
  x$spark <- x$python$version
  names_x <- names(x)
  names_p <- names_x[names_x != "con"]
  for (i in seq_along(names_p)) {
    cp <- names_p[i]
    val_x <- x[[cp]]
    if (val_x != "") {
      cli_li("{.val0 {cp}:} {.val1 {val_x}}")
    }
  }
}
