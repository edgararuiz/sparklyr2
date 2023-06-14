import_check <- function(x, virtualenv_name) {
  env_found <- virtualenv_name %in% virtualenv_list()

  env_loaded <- NA

  if (py_available()) {
    if (env_found) {
      if (virtualenv_python(virtualenv_name) == py_exe()) {
        env_loaded <- TRUE
      } else {
        env_loaded <- FALSE
      }
    }
  } else {
    if (env_found) {
      try(use_virtualenv(virtualenv_name), silent = TRUE)
    }
  }

  out <- try(import(x), silent = TRUE)

  if (is.na(env_loaded)) {
    env_loaded <- virtualenv_python(virtualenv_name) == py_exe()
  }

  inst <- paste0(
    " {.run sparklyr2::install_sparklyr2(",
    "virtualenv_name = \"{virtualenv_name}\")}"
  )

  if (inherits(out, "try-error")) {
    if (env_found) {
      if (env_loaded) {
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
        "virtual environment is not installed. Restart your R session,",
        "and run:", inst
      ))
    }
  } else {
    if (is.null(sparklyr2_env$vars$python_init)) {
      if (env_loaded) {
        msg <- "Using the '{virtualenv_name}' virtual environment ({py_exe()})"
        cli_alert_success(msg)
      } else {
        msg <- paste(
          "Not using the '{virtualenv_name}' virtual environment",
          "for python. The current path is: {py_exe()}"
        )
        cli_alert_danger(msg)
      }
      sparklyr2_env$vars$python_init <- 1
    }
  }

  out
}

get_con_id <- function() {
  x <- sparklyr2_env$vars$con_count
  if(is.null(x)) {
    out <- 1
  } else {
    out <- x + 1
  }
  sparklyr2_env$vars$con_count <- out
  as.character(out)
}
