# Non-exported utility functions

# An environment to hold connection state
.scidbenv = new.env()

# Map scidb object column classes into R, adding an extra integer at the start for the index!
scidbcc = function(x)
{
  if(!is.null(options("scidb.test")[[1]]))
  {
    cat("Using old method for data.frame import")
    return(NA)
  }
  c("integer",as.vector(unlist(lapply(.scidbtypes[scidb_types(x)],function(x) ifelse(is.null(x),NA,x)))))
}

.scidbstr = function(object)
{
  name = substr(object@name,1,35)
  if(nchar(object@name)>35) name = paste(name,"...",sep="")
  cat("SciDB expression ", name)
  cat("\nSciDB schema ", schema(object), "\n")
  bounds = scidb_coordinate_bounds(object)
  d = data.frame(variable=dimensions(object), dimension=TRUE, type="int64", nullable=FALSE, start=bounds$start, end=bounds$end, chunk=scidb_coordinate_chunksize(object), row.names=NULL, stringsAsFactors=FALSE)
  d = rbind(d, data.frame(variable=scidb_attributes(object),
                          dimension=FALSE,
                          type=scidb_types(object), nullable=scidb_nullable(object), start="", end="", chunk=""))
  cat(paste(capture.output(print(d)), collapse="\n"))
  cat("\n")
}


# Internal warning function
warnonce = (function() {
  state = list(
    count="Use `count` for an exact count of data rows.",
    nonum="Note: The R sparse Matrix package does not support certain value types like\ncharacter strings"
  )
  function(warn) {
    if(!is.null(state[warn][[1]])) {
      message(state[warn])
      s <<- state
      s[warn] = c()
      state <<- s
    }
  }
}) ()


# Utility function that parses an aggregation expression
# makes sense of statements like
# max(a) as amax, avg(b) as bmean, ...
# Input
# x: scidb object
# expr: aggregation expression (as is required)
# Output
# New attribute schema
aparser = function(x, expr)
{
  map = list(
    ApproxDC    = function(x) "double null",
    avg         = function(x) "double null",
    count       = function(x) "uint64 null",
    first_value = function(x) sprintf("%s null",x),
    last_value  = function(x) sprintf("%s null",x),
    mad         = function(x) sprintf("%s null",x),
    max         = function(x) sprintf("%s null",x),
    min         = function(x) sprintf("%s null",x),
    median      = function(x) sprintf("%s null",x),
    prod        = function(x) sprintf("%s null",x),
    stdev       = function(x) "double null",
    sum         = function(x) sprintf("%s null",x),
    top_five    = function(x) sprintf("%s null",x),
    var         = function(x) "double null")

  p = strsplit(expr,",")[[1]]                      # comma separated
  p = lapply(p,function(v) strsplit(v,"as")[[1]])  # as is required
  new_attrs = lapply(p, function(v) v[[2]])        # output attribute names
  y = lapply(p,function(v)strsplit(v,"\\(")[[1]])  #
  fun = lapply(y, function(v) gsub(" ","",v[[1]])) # functions
  old_attrs = unlist(lapply(y, function(v)gsub("[\\) ]", "", v[[2]])))
  i = 1:length(scidb_attributes(x))
  names(i) = scidb_attributes(x)
  old_types = scidb_types(x)[i[old_attrs]]
  old_types[is.na(old_types)] = "int64"    # catch all
  z = rep("", length(new_attrs))
  for(j in 1:length(z))
  {
    z[j] = sprintf("%s:%s", new_attrs[j],map[unlist(fun)][[j]](old_types[j]))
  }
  sprintf("<%s>", paste(z, collapse=","))
}


#' Nightmarish internal function that converts R expressions to SciDB expressions
#' @param expr an R 'language' type object to parse
#' @param sci a SciDB array 
#' @param frame to evaluate expression for replacement values
#' @keywords internal
#' @return character-valued SciDB expression
#' @importFrom codetools makeCodeWalker walkCode
rewrite_subset_expression = function(expr, sci, frame)
{
  dims = dimensions(sci)
  n = length(dims)
  template = rep("", 2 * n)
  DEBUG = FALSE
  if(!is.null(options("scidb.debug")[[1]]) && TRUE == options("scidb.debug")[[1]]) DEBUG=TRUE

  .toList = makeCodeWalker(call=function(e, w)
                                {
                                  tryCatch(eval(e, frame),
                                    error=function(err) lapply(e, walkCode, w))
                                },
                           leaf=function(e, w) e)

# Walk the ast for R expression x, annotating elements with identifying
# attributes. Specify a character vector of array dimension in dims.
# Substitute evaluated R scalars for variables where possible (but not more
# complext R expressions).  The output is a list that can be parsed by the
# `.compose_r` function below.
  .annotate = function(x, dims=NULL, attr=NULL, op="")
  {
    if(is.list(x))
    {
      if(length(x) > 1) op = c(op,as.character(x[[1]]))
      return(lapply(x, .annotate, dims, attr, op))
    }
    op = paste(op,collapse="")
    s = as.character(x)
    if(!(s %in% c(dims,attr, ">", "<", "!", "|", "=", "&", "||", "&&", "!=", "==", "<=", ">=")))
    {
      test = tryCatch(eval(x, frame), error=function(e) e)
      if(length(test) > 0)
      {
        test = test[!grepl("condition", lapply(test, class))]
        if(length(test) > 0)
        {
          if(DEBUG) cat("Replacing symbol", s, "with ")
          s = tryCatch(noE(test[[1]]), error=function(e) s)
          if(DEBUG) cat(s, "\n")
        }
      }
    }
    attr(s, "what") = "element"
    if(is.character(x))
    {
      attr(s, "what") = "character"
    }
    if(nchar(gsub("null", "", gsub("[0-9 -\\.]+", "", s, perl=TRUE), ignore.case=TRUE)) == 0)
      attr(s, "what") = "scalar"
    if(any(dims %in% gsub(" ", "", s)) && nchar(gsub("[&(<>=) ]*", "", op)) == 0)
    {
      attr(s, "what") = "dimension"
    }
    s
  }

  .compose_r = function(x)
  {
    if(is.list(x))
    {
      if(length(x)==3)
      {
        if(!is.list(x[[2]]) && !is.list(x[[3]]) &&
           all( c("scalar", "dimension") %in%
                c(attr(x[[2]],"what"), attr(x[[3]], "what"))))
        {
          b = template
          d = which(c(attr(x[[2]], "what"), attr(x[[3]], "what")) == "dimension") + 1
          s = which(c(attr(x[[2]], "what"), attr(x[[3]], "what")) == "scalar") + 1
          i = which(dims %in% x[[d]]) # template index
          intx = round(as.numeric(x[[s]]))
          if(intx >= 2 ^ 53) stop("Values too large, use an explicit SciDB filter expression")
          numx = as.numeric(x[[s]])
          lb = ceiling(numx)
          ub = floor(intx)
          if(intx==numx)
          {
            lb = intx + 1
            ub = intx - 1
          }
          if(x[[1]] == "==" || x[[1]] == "=") b[i] = b[i + n] = x[[s]]
          else if(x[[1]] == "<") b[i + n] = sprintf("%.0f", ub)
          else if(x[[1]] == "<=") b[i + n] = sprintf("%.0f", intx)
          else if(x[[1]] == ">") b[i] = sprintf("%.0f", lb)
          else if(x[[1]] == ">=") b[i] = sprintf("%.0f", intx)
          b = paste(b, collapse=",")
          return(sprintf("::%s", b))
        }
        return(c(.compose_r(x[[2]]), as.character(x[[1]]), .compose_r(x[[3]])))
      }
      if(length(x)==2)
      {
        if(as.character(x[[1]]) == "(") return(c(.compose_r(x[[1]]), .compose_r(x[[2]]), ")"))
        return(c(.compose_r(x[[1]]),.compose_r(x[[2]])))
      }
    }
    if(attr(x,"what") == "character") return(sprintf("'%s'", as.character(x)))
    as.character(x)
  }

  ans = .compose_r(.annotate(walkCode(expr, .toList), dims=dims, attr=scidb_attributes(sci)))
  i   = grepl("::",ans)
  ans = gsub("==", "=", gsub("!", "not", gsub("\\|", "or", gsub("\\|\\|", "or",
          gsub("&", "and", gsub("&&", "and", gsub("!=", "<>", ans)))))))
  if(any(i))
  {
# Compose the betweens in a highly non-elegant way
    b = strsplit(paste(gsub("::", "", ans[i]), ""), ",")
    ans[i] = "true"
    b = Reduce(function(x, y)
    {
      n = length(x)
      x = tryCatch(as.numeric(x), warning=function(e) rep(NA,n))
      y = tryCatch(as.numeric(y), warning=function(e) rep(NA,n))
      m = n / 2
      x1 = x[1:m]
      y1 = y[1:m]
      x1[is.na(x1)] = -Inf
      y1[is.na(y1)] = -Inf
      x1 = pmax(x1,y1)
      x2 = x[(m+1):n]
      y2 = y[(m+1):n]
      x2[is.na(x2)] = Inf
      y2[is.na(y2)] = Inf
      x2 = pmin(x2,y2)
      c(x1, x2)
    }, b, init=template)
    b = gsub("Inf", "null", gsub("-Inf", "null", sprintf("%.0f",b)))
    ans = gsub("and true", "", paste(ans, collapse=" "))
    if(nchar(gsub(" ", "", ans)) == 0 || gsub(" ", "", ans) == "true")
      return(sprintf("between(%s,%s)", sci@name,paste(b, collapse=",")))
    return(sprintf("filter(between(%s,%s),%s)", sci@name, paste(b, collapse=","), ans))
  }
  sprintf("filter(%s,%s)", sci@name, paste(ans, collapse=" "))
}


# Internal function
create_temp_array = function(name, schema)
{
# SciDB temporary array syntax varies with SciDB version
  TEMP = "'TEMP'"
  if(compare_versions(options("scidb.version")[[1]], 14.12)) TEMP="true"
  query   = sprintf("create_array(%s, %s, %s)", name, schema, TEMP)
  iquery(query, `return`=FALSE)
}


# An important internal convenience function that returns a scidb object.  If
# eval=TRUE, a new SciDB array is created the returned scidb object refers to
# that.  Otherwise, the returned scidb object represents a SciDB array promise.
#
# INPUT
# expr: (character) A SciDB expression or array name
# eval: (logical) If TRUE evaluate expression and assign to new SciDB array.
#                 If FALSE, infer output schema but don't evaluate.
# name: (optional character) If supplied, name for stored array when eval=TRUE
# gc: (optional logical) If TRUE, tie SciDB object to  garbage collector.
# depend: (optional list) An optional list of other scidb objects
#         that this expression depends on (preventing their garbage collection
#         if other references to them go away).
# schema, temp: (optional) used to create SciDB temp arrays
#               (requires scidb >= 14.8)
#
# OUTPUT
# A `scidb` array object.
#
# NOTE
# Only AFL supported.
`.scidbeval` = function(expr, eval=FALSE, name, gc=TRUE, depend, schema, temp)
{
  ans = c()
  if(missing(depend)) depend = c()
  if(missing(schema)) schema = ""
  if(missing(temp)) temp = FALSE
  if(!is.list(depend)) depend = list(depend)
# Address bug #45. Try to cheaply determine if expr refers to a named array
# or an AFL expression. If it's a named array, then eval must be set TRUE.
  if(!grepl("\\(", expr, perl=TRUE)) eval = TRUE
  if(`eval`)
  {
    if(missing(name) || is.null(name))
    {
      newarray = tmpnam()
      if(temp) create_temp_array(newarray, schema)
    }
    else newarray = name
    query = sprintf("store(%s,%s)", expr, newarray)
    iquery(query, `return`=FALSE)
    ans = scidb(newarray, gc=gc)
    if(temp) ans@gc$temp = TRUE
# This is a fix for a SciDB issue that can unexpectedly change schema
# bounds. And another fix to allow unexpected dimname and attribute name
# changes. Arrgh.
#    if(schema != "" && !compare_schema(ans, schema, ignore_attributes=TRUE, ignore_dimnames=TRUE))
#    {
#      ans = repart(ans, schema)
#    }
  } else
  {
    ans = scidb(expr, gc=gc)
# Assign dependencies
    if(length(depend) > 0)
    {
      assign("depend", depend, envir=ans@gc)
    }
  }
  ans
}

make.names_ = function(x)
{
  gsub("\\.", "_", make.names(x, unique=TRUE), perl=TRUE)
}

# x is vector of existing values
# y is vector of new values
# returns a set the same size as y with non-conflicting value names
make.unique_ = function(x, y)
{
  z = make.names(gsub("_", ".", c(x,y)), unique=TRUE)
  gsub("\\.", "_", tail(z, length(y)))
}

# Make a name from a prefix and a unique SciDB identifier.
tmpnam = function(prefix="R_array")
{
  salt = basename(tempfile(pattern=prefix))
  if(!exists("uid",envir=.scidbenv)) stop("Not connected...try scidbconnect")
  paste(salt,get("uid",envir=.scidbenv),sep="")
}


# Check if array exists
.scidbexists = function(name)
{
  Q = scidblist()
  return(name %in% Q)
}

# Sparse matrix to SciDB
.Matrix2scidb = function(X,name,rowChunkSize=1000,colChunkSize=1000,start=c(0,0),gc=TRUE,...)
{
  D = dim(X)
  N = Matrix::nnzero(X)
  rowOverlap=0L
  colOverlap=0L
  if(length(start)<1) stop ("Invalid starting coordinates")
  if(length(start)>2) start = start[1:2]
  if(length(start)<2) start = c(start, 0)
  start = as.integer(start)
  type = .scidbtypes[[typeof(X@x)]]
  if(is.null(type)) {
    stop(paste("Unupported data type. The package presently supports: ",
       paste(.scidbtypes,collapse=" "),".",sep=""))
  }
  if(type!="double") stop("Sorry, the package only supports double-precision sparse matrices right now.")
  schema = sprintf(
      "< val : %s null>  [i=%.0f:%.0f,%.0f,%.0f, j=%.0f:%.0f,%.0f,%.0f]", type, start[[1]],
      nrow(X)-1+start[[1]], min(nrow(X),rowChunkSize), rowOverlap, start[[2]], ncol(X)-1+start[[2]],
      min(ncol(X),colChunkSize), colOverlap)
  schema1d = sprintf("<i:int64 null, j:int64 null, val : %s null>[idx=0:*,100000,0]",type)

# Obtain a session from shim for the upload process
  session = getSession()
  if(length(session)<1) stop("SciDB http session error")
  on.exit(SGET("/release_session",list(id=session), err=FALSE) ,add=TRUE)

# Compute the indices and assemble message to SciDB in the form
# double,double,double for indices i,j and data val.
  dp = diff(X@p)
  j  = rep(seq_along(dp),dp) - 1

stop("not supported")

# Upload the data
#  bytes = .Call("scidb_raw",as.vector(t(matrix(c(X@i + start[[1]],j + start[[2]], X@x),length(X@x)))),PACKAGE="scidb")

# redimension into a matrix
#  query = sprintf("store(redimension(input(%s,'%s',-2,'(double null,double null,double null)'),%s),%s)",schema1d, ans, schema, name)
#  iquery(query, `return`=FALSE)
#  scidb(name, gc=gc)
}


# Check for scidb missing flag
is.nullable = function(x)
{
  any(scidb_nullable(x))
}

# Internal utility function, make every attribute of an array nullable
make_nullable = function(x)
{
  cast(x, sprintf("%s%s", build_attr_schema(x, nullable=TRUE), build_dim_schema(x)))
}

# Internal utility function used to format numbers
noE = function(w) sapply(w,
  function(x)
  {
    if(is.infinite(x)) return("*")
    if(is.character(x)) return(x)
    sprintf("%.0f", x)
  })

# Returns TRUE if version string x is greater than or equal to than version y
compare_versions = function(x, y)
{
  b = as.numeric(strsplit(as.character(x), "\\.")[[1]])
  a = as.numeric(strsplit(as.character(y), "\\.")[[1]])
  ans = b[1] > a[1]
  if(b[1] == a[1]) ans = b[2] >= a[2]
  ans
}

# Used in delayed assignment of scidb object schema and logical_plan values.
lazyeval = function(name)
{
  escape = gsub("'", "\\\\'", name, perl=TRUE)
# SciDB explain_logical operator changed in version 15.7
  if(compare_versions(options("scidb.version")[[1]], 15.7))
  {
    query = sprintf("join(show('filter(%s,true)','afl'), _explain_logical('filter(%s,true)','afl'))", escape, escape)
  }
  else
  {
    query = sprintf("join(show('filter(%s,true)','afl'), explain_logical('filter(%s,true)','afl'))", escape, escape)
  }
  query = iquery(query, `return`=TRUE)
  list(schema = gsub("^.*<", "<", query$schema, perl=TRUE),
       logical_plan = query$logical_plan)
}
