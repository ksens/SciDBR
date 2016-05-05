#' Upload R data to SciDB
#' Data are uploaded as TSV.
#' @param X an R data frame, raw value, Matrix, matrix, or vector object
#' @param name a SciDB array name to use
#' @param start starting SciDB integer coordinate index
#' @param gc set to FALSE to disconnect the SciDB array from R's garbage collector
#' @param ... other options, see \code{\link{df2scidb}}
#' @return A \code{scidb} object
#' @export
as.scidb = function(X,
                    name=tmpnam(),
                    start,
                    gc=TRUE, ...)
{
  if(inherits(X, "raw"))
  {
#    return(raw2scidb(X, name=name, gc=gc,...))
  }
  if(inherits(X, "data.frame"))
  {
    return(df2scidb(X, name=name, gc=gc, start=start, ...))
  }
  if(inherits(X, "dgCMatrix"))
  {
    return(.Matrix2scidb(X, name=name, start=start, gc=gc, ...))
  }
  return(matvec2scidb(X, name=name, start=start, gc=gc, ...))
}

#' Internal function to upload an R data frame to SciDB
#' @param X a data frame
#' @param name SciDB array name
#' @param dimlabel name of SciDB dimension
#' @param chunkSize SciDB chunk size
#' @param rowOverlap SciDB chunk overlap
#' @param types SciDB attribute types
#' @param nullable SciDB attribute nullability
#' @param schema_only set to \code{TRUE} to just return the SciDB schema (don't upload)
#' @param gc set to \code{TRUE} to connect SciDB array to R's garbage collector
#' @param start SciDB coordinate index starting value
#' @return A \code{\link{scidb}} object, or a character schema string if \code{schema_only=TRUE}.
#' @keywords internal
df2scidb = function(X,
                    name=tmpnam(),
                    dimlabel="row",
                    chunkSize,
                    rowOverlap=0L,
                    types=NULL,
                    nullable,
                    schema_only=FALSE,
                    gc, start)
{
  stop("not yet supported")
}

#' Fast write.table/textConnection substitute
#'
#' Conversions are vectorized and the entire output is buffered in memory and written in
#' one shot. Great option for replacing writing to a textConnection (much much faster).
#' Not such a great option for writing to files, only 10% faster than write.table and
#' obviously much greater memory use.
#' @param x a data frame
#' @param file a connection or \code{return} to return character output directly (fast)
#' @param sep column separator
#' @param format optional fprint-style column format specifyer
#' @return Use for the side effect of writing to the connection returning \code{NULL}, or
#' return a character value when \code{file=return}.
#' @keywords internal
fwrite = function(x, file=stdout(), sep="\t", format=paste(rep("%s", ncol(x)), collapse=sep))
{
  if(!is.data.frame(x)) stop("x must be a data.frame")
  if(is.function(file)) return(paste(do.call("sprintf", args=c(format, as.list(x))), collapse="\n"))
  write(paste(do.call("sprintf", args=c(format, as.list(x))), collapse="\n"), file=file)
  invisible()
}

matvec2scidb = function(X,
                        name=tmpnam(),
                        start,
                        gc=TRUE, ...)
{
# Check for a bunch of optional hidden arguments
  args = list(...)
  attr_name = "val"
  nullable = TRUE
  if(!is.null(args$nullable)) nullable = as.logical(args$nullable) # control nullability
  if(!is.null(args$attr)) attr_name = as.character(args$attr)      # attribute name
  do_reshape = TRUE
  nd_reshape = NULL
  type = force_type = .Rtypes[[typeof(X)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(unique(names(.Rtypes)), collapse=" "), ".", sep=""))
  }
  if(!is.null(args$reshape)) do_reshape = as.logical(args$reshape) # control reshape
  if(!is.null(args$type)) force_type = as.character(args$type) # limited type conversion
  chunkSize = c(min(1000L, nrow(X)), min(1000L, ncol(X)))
  chunkSize = as.numeric(chunkSize)
  if(length(chunkSize) == 1) chunkSize = c(chunkSize, chunkSize)
  overlap = c(0,0)
  if(missing(start)) start = c(0,0)
  start     = as.numeric(start)
  if(length(start) ==1) start = c(start, start)
  D = dim(X)
  start = as.integer(start)
  overlap = as.integer(overlap)
  dimname = make.unique_(attr_name, "i")
  if(is.null(D))
  {
# X is a vector
    if(!is.vector(X)) stop ("Unsupported object")
    do_reshape = FALSE
    chunkSize = min(chunkSize[[1]], length(X))
    X = as.matrix(X)
    schema = sprintf(
        "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, dimname, start[[1]],
        nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = schema
  } else if(length(D) > 2)
  {
    nd_reshape = dim(X)
    do_reshape = FALSE
    X = as.matrix(as.vector(aperm(X)))
    schema = sprintf(
        "< %s : %s null>  [%s=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, dimname, start[[1]],
        nrow(X) - 1 + start[[1]], min(nrow(X), chunkSize), overlap[[1]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, force_type, length(X))
  } else {
# X is a matrix
    schema = sprintf(
      "< %s : %s  null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", attr_name, force_type, start[[1]],
      nrow(X) - 1 + start[[1]], chunkSize[[1]], overlap[[1]], start[[2]], ncol(X) - 1 + start[[2]],
      chunkSize[[2]], overlap[[2]])
    load_schema = sprintf("<%s:%s null>[__row=1:%.0f,1000000,0]", attr_name, force_type,  length(X))
  }
  if(!is.matrix(X)) stop ("X must be a matrix or a vector")

stop("not supported yet")

}
