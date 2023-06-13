rstudio_objects <- function(sc,
                            catalog = NULL,
                            schema = NULL,
                            name = NULL,
                            type = NULL
                            ) {
  df_catalogs <- data.frame()
  df_databases <- data.frame()
  df_tables <- data.frame()
  df_cat <- data.frame()

  sc_catalog <- sc$python$catalog
  current_catalog <- sc_catalog$currentCatalog()
  if(is.null(catalog)) {

    sc_catalog$setCurrentCatalog("spark_catalog")
    tables <- sc_catalog$listTables(dbName = "default")
    if(length(tables) > 0) {
      temps <- tables[map_lgl(tables, ~.x$isTemporary)]
      if(length(temps) > 0) {
        df_tables <- data.frame(name = map_chr(temps, ~ .x$name))
        df_tables$type = "table"
      }
      df_tables <- rbind(df_tables, df_cat)
    }

    catalogs <- sc_catalog$listCatalogs()
    if(length(catalogs) > 0) {
      df_catalogs <- data.frame(name = map_chr(catalogs, ~ .x$name))
      df_catalogs$type <- "catalog"
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

        catalogs <- map(tables, ~.x$catalog == catalog)
        catalogs <- map_lgl(catalogs, ~ifelse(length(.x), .x, FALSE))
        tables <- tables[catalogs]

        schemas <- map(tables, ~.x$namespace == schema)
        schemas <- map_lgl(schemas, ~ifelse(length(.x), .x, FALSE))
        tables <- tables[schemas]

        if(length(tables) > 0) {
          df_tables <- data.frame(name = map_chr(tables, ~ .x$name))
          df_tables$type = "table"
        }
      }
      out <- df_tables
    }

  }

  sc_catalog$setCurrentCatalog(current_catalog)
  out
}

rstudio_columns <- function(sc,
                            table = NULL,
                            view = NULL,
                            catalog = NULL,
                            schema = NULL
                            ) {
  tbl_df <- rs_get_table(sc, catalog, schema, table)

  tbl_sample <- collect(head(tbl_df))

  tbl_info <- map_chr(tbl_sample, ~ paste0(rs_type(.x), " ", rs_vals(.x)))

  data.frame(
    name = names(tbl_info),
    type = tbl_info
  )

}

rstudio_preview <- function(sc,
                            rowLimit,
                            table = NULL,
                            view = NULL,
                            catalog = NULL,
                            schema = NULL
                            ) {
  tbl_df <- rs_get_table(sc, catalog, schema, table)
  collect(head(tbl_df, rowLimit))
}

rs_get_table <- function(sc, catalog, schema, table) {
  if(is.null(catalog)) {
    catalog <- sc$python$catalog$currentCatalog()
  }
  if(is.null(schema)) {
    schema <- sc$python$catalog$currentDatabase()
  }
  x <- in_catalog(catalog, schema, table)
  if(!sc$python$catalog$tableExists(as.sql(x, sc$con))) {
    x <- table
  }
  tbl(sc, x)
}

rs_type <- function(x) {
  class <- class(x)[[1]]
  if(class == "integer") class <- "int"
  if(class == "numeric") class <- "num"
  if(class == "POSIXct") class <- "dttm"
  if(class == "character") class <- "chr"
  class
}

rs_vals <- function(x) {
  ln <- 30
  x <- paste0(x, collapse = " ")
  if(nchar(x) > ln) {
    x <- substr(x, 1, (ln-3))
    x <- paste0(x, "...")
  }
  x
}

rstudio_open_connection <- function(sc) {

  display_name <- "Spark"
  if(sc$method == "spark_connect") {
    display_name <- glue("Spark Connect - {sc$remote}")
    code <- glue("(host = \"{sc$remote}\")")
  }
  if(sc$method == "db_connect") {
    display_name <- glue("Databricks Connect - Cluster: {sc$cluster_id})")
    code <- glue("(\n host = \"{sc$remote}\", \n cluster_id = \"{sc$cluster_id}\"\n)")
  }

  code <- paste0("library(sparklyr2)\nsc <- spark_connect", code)

  contract <- rscontract_ide()
  contract$connectionObject <- sc
  contract$host <- sc$remote
  contract$type <- "Spark"
  contract$displayName <- display_name
  contract$connectCode <- code
  contract$previewObject <- function(rowLimit, ...) {
    rstudio_preview(sc, rowLimit, ...)
  }
  contract$listObjects <- function(...) {
    rstudio_objects(sc, ...)
  }
  contract$disconnect <- function() {
    spark_disconnect(sc)
  }
  contract$listColumns <- function(...) {
    rstudio_columns(sc, ...)
  }
  rscontract_open(contract)
}

rstudio_update_connection <- function(sc) {
  rscontract_update(sc$remote, type = "Spark")
}

rstudio_close_connection <- function(sc) {
  rscontract_close(sc$remote, type = "Spark")
}

