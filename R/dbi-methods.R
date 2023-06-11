#' Driver for Sparklyr2 database.
#'
#' @keywords internal
#' @export
#' @import DBI
#' @import methods
setClass("Sparklyr2Driver", contains = "DBIDriver")

setMethod("show", "Sparklyr2Driver", function(object) {
  cat("<Sparklyr2Driver>\n")
})

#' DBI integration
#' @export
dbi_driver <- function() {
  new("Sparklyr2Driver")
}

#' Sparklyr2 connection class.
#'
#' @export
#' @keywords internal
setClass("Sparklyr2Connection",
         contains = "DBIConnection",
         slots = list(
           host = "character",
           ptr = "externalptr"
         )
)

#' @param drv An object created by \code{Sparklyr2()}
#' @rdname dbi_driver
#' @export
#' @examples
#' \dontrun{
#' sc <- dbConnect(sparklyr2::dbi_driver())
#' dbWriteTable(sc, "mtcars", mtcars)
#' dbGetQuery(sc, "SELECT * FROM mtcars WHERE cyl == 4")
#' }
setMethod("dbConnect", "Sparklyr2Driver",
          function(drv, host = "sc://localhost", ...) {
            new("Sparklyr2Connection", host = host, ...)
            }
          )
