rstudio_catalog <- function(sc) {

  list_catalogs <- sc$python$catalog$listCatalogs()

  sc$python$catalog$setCurrentCatalog("spark_catalog")
  tables <- sc$python$catalog$listTables(dbName = "default")

  tables <- map(tables, function(x) {
    tbl_x <- sc$python$table(x$name)
    fields <- map(tbl_x$columns, function(x) {
      list(
        name = x,
        type = "??"
      )
    })

    list(
      table = list(
        name = x$name,
        type = "table"
      )
    )
  })

  list(
    name = "Spark Session",
    type = "catalog",
    schemas = list(
      name = "Storage",
      type = "schema",
      tables = flatten(tables)
    )
  )
}

rstudio_objects <- function(sc,
                            catalog = NULL,
                            schema = NULL,
                            name = NULL,
                            type = NULL
                            ) {
  df_catalogs <- data.frame()
  df_databases <- data.frame()
  df_tables <- data.frame()

  sc_catalog <- sc$python$catalog
  current_catalog <- sc_catalog$currentCatalog()
  if(is.null(catalog)) {
    sc_catalog$setCurrentCatalog("spark_catalog")
    tables <- sc_catalog$listTables(dbName = "default")

    catalogs <- sc_catalog$listCatalogs()
    if(length(catalogs) > 0) {
      df_catalogs <- data.frame(name = map_chr(catalogs, ~ .x$name))
      df_catalogs$type <- "catalog"
    }

    if(length(tables) > 0) {
      df_tables <- data.frame(name = map_chr(tables, ~ .x$name))
      df_tables$type = "table"
    }
    out <- rbind(df_tables, df_catalogs)
  } else {
    sc_catalog$setCurrentCatalog(catalog)
    if(is.null(schema)) {
      databases <- sc_catalog$listDatabases()
      df_databases <- data.frame(name = map_chr(databases, ~ .x$name))
      df_databases$type <- "schema"
      out <- df_databases
    } else {
      tables <- sc_catalog$listTables(dbName = schema)
      if(length(tables) > 0) {
        df_tables <- data.frame(name = map_chr(tables, ~ .x$name))
        df_tables$type = "table"
      }
      out <- df_tables
    }

  }

  sc_catalog$setCurrentCatalog(current_catalog)
  out
}

rstudio_open_connection <- function(sc) {
  contract <- rscontract_ide()
  contract$connectionObject <- sc
  contract$host <- sc$remote
  contract$type <- "Spark"
  contract$listObjects <- function(...) {
    rstudio_objects(sc, ...)
  }
  contract$disconnect <- function(...) spark_disconnect(sc)
  rscontract_open(contract)
}

rstudio_update_connection <- function(sc) {
  rscontract_update(sc$remote, type = "Spark")
}

rstudio_close_connection <- function(sc) {
  rscontract_update(sc$remote, type = "Spark")
}

