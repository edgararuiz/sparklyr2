#' Connects to a Spark environment
#' @param host The URL or path for the master, or the remote, parameter
#' @param token Credentials token. Required for Databricks Connect.
#' @param cluster_id Cluster ID to connect to. Required for Databricks Connect.
#' @param method Method to be used for connecting. "auto" will attempt to infer
#' what kind of method to use based on the provided arguments.
#' @export
spark_connect <- function(host,
                          token = Sys.getenv("DATABRICKS_TOKEN"),
                          cluster_id = NULL,
                          method = c("auto", "spark_connect", "db_connect")
                          ) {
  master <- ""
  remote <- ""

  if(method[[1]] == "auto") {
    if(is.null(cluster_id) & grepl("sc://", host)) {
      method <- "spark_connect"
    }
    if(!is.null(cluster_id)) {
      method <- "db_connect"
    }
  }

  if("sparklyr2" %in% virtualenv_list()) {
    try(use_virtualenv("sparklyr2"), silent = TRUE)
  }

  if (method == "spark_connect") {
    pyspark <- import("pyspark")
    pyspark_sql <- pyspark$sql
    remote <- pyspark_sql$SparkSession$builder$remote(host)
    python <- remote$getOrCreate()
    con_class <- "sparklyr2_connect"
    remote <- host
  }

  if(method == "db_connect") {
    db <- import("databricks.connect")
    remote <- db$DatabricksSession$builder$remote(
      host = host,
      token = token,
      cluster_id = cluster_id
    )
    python <- remote$getOrCreate()
    con_class <- "sparklyr2_db"
    remote <- host
  }

  sc <- structure(
    list(
      master = master,
      remote = remote,
      cluster_id = cluster_id,
      method = method,
      python = python,
      con = simulate_hive()
    ),
    class = c("sparklyr2_connection", con_class, "DBIConnection")
  )
  rstudio_open_connection(sc)
  sc
}

#' Disconnects from a Spark environment
#' @param sc Spark connection
#' @export
spark_disconnect <- function(sc) {
  sc$python$client$close()
  rscontract_close(sc$remote, type = "Spark")
}

#' @export
print.sparklyr2_connect <- function(x, ...) {
  cli_div(theme = cli_colors())
  cli_h3("{.val2 sparklyr2} - {.val1 Spark Connect} -")
  print_connection(x)
}

#' @export
print.sparklyr2_db <- function(x, ...) {
  cli_div(theme = cli_colors())
  cli_h3("{.val2 sparklyr2} - {.val1 Databricks Connect} -")
  print_connection(x)
}

print_connection <- function(x) {
  cli_div(theme = cli_colors())
  x$spark <- x$python$version
  names_x <- names(x)
  names_p <- names_x[names_x != "con"]
  names_p <- names_p[names_p != "method"]
  for (i in seq_along(names_p)) {
    cp <- names_p[i]
    val_x <- x[[cp]]
    if(is.null(val_x)) val_x <- ""
    if (val_x != "") {
      cli_li("{.val0 {cp}:} {.val1 {val_x}}")
    }
  }
}
