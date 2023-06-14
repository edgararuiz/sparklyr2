#' Connects to a Spark environment
#' @param host The URL or path for the master, or the remote, parameter
#' @param token Credentials token. Required for Databricks Connect.
#' @param cluster_id Cluster ID to connect to. Required for Databricks Connect.
#' @param method Method to be used for connecting. "auto" will attempt to infer
#' what kind of method to use based on the provided arguments.
#' @param virtualenv_name The name of the Virtual Environment to use to
#' install the python libraries. Defaults to "sparklyr2".
#' @export
spark_connect <- function(host,
                          token = Sys.getenv("DATABRICKS_TOKEN"),
                          cluster_id = NULL,
                          method = c("auto", "spark_connect", "db_connect"),
                          virtualenv_name = "sparklyr2"
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

  if (method == "spark_connect") {
    pyspark <- import_check("pyspark", virtualenv_name)
    pyspark_sql <- pyspark$sql
    remote <- pyspark_sql$SparkSession$builder$remote(host)
    python <- remote$getOrCreate()
    con_class <- "sparklyr2_connect"
    remote <- host
  }

  if(method == "db_connect") {
    db <- import_check("databricks.connect", virtualenv_name)
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

#' Installs python dependencies
#' @param python_version The version of python to install if not available
#' @param virtualenv_name The name of the Virtual Environment to use to
#' install the python libraries. Defaults to "sparklyr2".
#' @export
install_sparklyr2 <- function(python_version = NULL,
                              virtualenv_name = "sparklyr2"
                              ) {

  if(!py_available()) {
    if(is.null(python_version)) {
      python_path <- install_python()
    } else {
      python_path <- install_python(version = python_version)
    }
    use_python(python = python_path)
  }

  if(!(virtualenv_name %in% virtualenv_list())) {
    virtualenv_create(virtualenv_name, package = NULL)
  }

  use_virtualenv(virtualenv_name)

  py_install(
    envname = virtualenv_name,
    packages = sparklyr2_env$vars$python_deps
    )

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

import_check <- function(x, virtualenv_name) {

  env_found <- virtualenv_name %in% virtualenv_list()

  env_loaded <- NA

  if(py_available()) {
    if(env_found) {
      if(virtualenv_python(virtualenv_name) == py_exe()) {
        env_loaded <- TRUE
      } else {
        env_loaded <- FALSE
      }
    }
  } else {
    if(env_found) {
      try(use_virtualenv(virtualenv_name), silent = TRUE)
    }
  }

  out <- try(import(x), silent = TRUE)

  if(is.na(env_loaded)) {
    env_loaded <- virtualenv_python(virtualenv_name) == py_exe()
    }

  inst <- " {.run sparklyr2::install_sparklyr2(virtualenv_name = \"{virtualenv_name}\")}"

  if(inherits(out, "try-error")) {
    if(env_found) {
      if(env_loaded) {
        # found & loaded
        cli_abort(paste(
          "Pyhon library '{x}' is not available in the '{virtualenv_name}'",
          "virtual environment. Install all of the needed python libraries",
          "using:", inst
        ))
      } else {
        cli_abort(paste(
          "Pyhon library '{x}' is not available. The '{virtualenv_name}'",
          "virtual environment is installed, but it is not loaded.",
          "Restart your R session, and avoid initializing Python",
          "before using `sparklyr2`"
        ))
      }
    } else {
      cli_abort(paste(
        "Pyhon library '{x}' not available. The '{virtualenv_name}'",
        "virtual environment is not installed. Restart your R session",
        "and run:", inst
      ))
    }
  } else {
    if(is.null(sparklyr2_env$vars$python_init)) {
      if(env_loaded) {
        msg <- "Using the '{virtualenv_name}' virtual environment ({py_exe()})"
        cli_alert_success(msg)
      } else {
        msg <- paste("Not using the '{virtualenv_name}' virtual environment",
                     "for python. The current path is: {py_exe()}"
                     )
        cli_alert_danger(msg)
      }
      sparklyr2_env$vars$python_init <- 1
    }
  }

  out
}
