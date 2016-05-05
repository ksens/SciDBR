#' Connect to SciDB
#' @param host I.P. address or host name
#' @param port SciDB server port number
#' @param user SciDB authentication
#' @param passwd SciDB authentication
#' @param drv full path to SciDB JDBC driver jar file
#' @return JDBC database connection object is invisibly returned, connection is also stored in global package environment.
#' @importFrom RJDBC JDBC dbConnect
#' @importFrom rJava .jcall
#' @export
scidbconnect = function(host="127.0.0.1",
                        port=1239L,
                        user, passwd,
                        drv=system.file("java/scidb4j.jar", package="scidb"))
{
  driver = JDBC("org.scidb.jdbc.Driver", drv)
  assign("drv", driver, envir=.scidbenv)
  if(missing(user) || missing(passwd))
    assign("conn", dbConnect(driver, "jdbc:scidb://localhost"), envir=.scidbenv)
  else
    assign("conn", dbConnect(driver, "jdbc:scidb://localhost", user=user, password=passwd), envir=.scidbenv)

# Make AFL the default
  conn = get("conn", envir=.scidbenv)
  scidb = .jcall(conn@jc, "Lorg/scidb/client/Connection;", "getSciDBConnection")
  .jcall(scidb, "V", "setAfl", TRUE)
}

#' Run a SciDB query, optionally returning the result in a \code{data.frame}.
#' @param query a single SciDB query string
#' @param return if \code{TRUE}, return the result
#' @param n optional limit on number of results returned, set to -1 to return everything
#' @importFrom RJDBC dbGetQuery dbSendQuery dbClearResult
#' @importFrom DBI dbFetch
#' @export
iquery = function(query, `return`=TRUE, n=-1)
{
  conn = .scidbenv$conn
  if(is.null(conn)) stop("not connected, try `scidbconnect`")
  x = dbSendQuery(conn, query)
  if(`return`)
  {
    m = .jcall(x@jr, "Ljava/sql/ResultSetMetaData;", "getMetaData") # metadaa
    j = .jcall(m, "I", "getColumnCount")
    types = vapply(1L:j, function(i) .jcall(m, "S", "getColumnTypeName", i), "")
    ans = dbFetch(x, n)
    dbClearResult(x)
    return(type_convert(ans, types))
  }
  invisible(dbClearResult(x))
}

#' Internal function that converts from SciDB types to R types
#' @param x a data frame
#' @param types a string vector of SciDB type names
#' @keywords internal
type_convert = function(x, types)
{
  newtypes = .scidbtypes[types]
  for(j in 1:ncol(x))
  {
    if(typeof(x[, j]) != newtypes[j])
    {
      x[, j] = tryCatch(switch(newtypes[[j]], 
        double=as.double(x[, j]),
        integer=as.integer(x[, j]),
        logical=as.logical(x[, j]),
        as.character(x[, j])), error=function(e) as.character(x[, j])
      )
    }
  }
  x
}
