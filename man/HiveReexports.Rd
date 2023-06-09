% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/methods-dbi.R
\docType{class}
\name{HiveReexports}
\alias{HiveReexports}
\alias{dbQuoteString,Hive,character-method}
\alias{dbQuoteIdentifier,Hive,character-method}
\title{Re-exports Hive DBI methods from \code{odbc}}
\usage{
\S4method{dbQuoteString}{Hive,character}(conn, x, ...)

\S4method{dbQuoteIdentifier}{Hive,character}(conn, x, ...)
}
\arguments{
\item{conn}{A \linkS4class{DBIConnection} object, as returned by
\code{\link[DBI:dbConnect]{dbConnect()}}.}

\item{x}{A character vector to quote as string.}

\item{...}{Other arguments passed on to methods.}
}
\description{
Re-exports Hive DBI methods from \code{odbc}
}
