## squr functions ---------------------------------------------------------

# Obtained from https://github.com/smbache/squr

# is_scalar_character -----------------------------------------------------

is_scalar_character <- function(value)
{
  is.character(value) && length(value) == 1
}


# as_symbol ---------------------------------------------------------------

as_symbol <- function(value)
{
  if (inherits(value, "AsIs"))
    value
  else if (inherits(value, "formula") && length(value) == 2)
    value[[2L]]
  else if (is_scalar_character(value))
    as.symbol(value)
  else
    stop("Cannot coerce to a symbol.")
}


# dbi_interpolate ---------------------------------------------------------

dbi_interpolate <- function(value, quote = NULL)
{
  if (length(value) > 1) {
    map_character(value, dbi_interpolate)
  } else {
    right_quote <-
      if (!is.null(quote))
        switch(quote,
               "[" = "]",
               '"' = '"',
               "'" = "'",
               stop("Invalid quote character."))
    
    out <- DBI::sqlInterpolate(DBI::ANSI(), "?value", value = value)
    
    if (!is.null(quote)) {
      gsub("^[^[:alnum:]]",
           quote,
           gsub("[^[:alnum:]]$", right_quote, out))
    } else {
      out
    }
  }
}


# eval_sq -----------------------------------------------------------------

eval_sq <- function(symbol, envir, enclos)
{
  if (inherits(symbol, "AsIs") || inherits(symbol, "sq_value"))
    sq_value(symbol)
  else
    sq_value(eval(
      expr   = substitute(s, list(s = symbol)),
      envir  = envir,
      enclos = enclos
    ))
}


# leaf_nodes --------------------------------------------------------------

leaf_nodes <- function(list.)
{
  if (length(list.) == 0)
    return(NULL)
  
  l <- list.[1]
  if (length(l[[1]]) > 1)
    c(leaf_nodes(as.list(l[[1]])), leaf_nodes(list.[-1]))
  else
    c(as.list(l[1]), leaf_nodes(list.[-1]))
}


# map ---------------------------------------------------------------------

map_character <- function(values, f = as.character, ...)
{
  vapply(values, f, character(1), ...)
}

map_integer <- function(values, f = as.integer, ...)
{
  vapply(values, f, integer(1), ...)
}


# methods -----------------------------------------------------------------

print.sq <- function(x, ...)
{
  if (!inherits(x, "sq") || !is.character(x))
    stop("Invalid sq object.")
  
  cat(x)
  
  invisible(x)
}

print.sq_value <- function(x, ...)
{
  if (!inherits(x, "sq_value") || !is.character(x))
    stop("Invalid sq_value object.")
  
  cat("sq_value:\n")
  
  print(unclass(x))
  invisible(x)
}

`+.sq` <- function(e1, e2)
{
  sq_text(paste(e1, e2, sep = "\n"))
}

# remove_ignore_blocks ----------------------------------------------------

remove_ignore_blocks <- function(sql)
{
  gsub("^(?:[\t ]*(?:\r?\n|\r))+",
       "",
       gsub("--rignore.*?--end", "", sql),
       perl = TRUE)
}

# append_sql_extension ----------------------------------------------------

append_sql_extension <- function(path)
{
  `if`(grepl(".*\\.sql$", path, ignore.case = TRUE),
       path,
       paste0(path, ".sql"))
}


# sq_value ----------------------------------------------------------------

sq_value <- function(value, quote = NULL)
{
  if (inherits(value, "sq_value"))
    return(value)
  
  if (is.list(value)) {
    out <-
      paste0("(", paste(
        vapply(value, sq_value, character(1), quote = quote),
        collapse = ","
      ), ")")
  } else {
    out <- rep("NULL", length(value))
    available <- !is.na(value)
    
    if (any(available)) {
      if (is.numeric(value)) {
        out[available] <- as.character(value[available])
      } else {
        out[available] <-
          dbi_interpolate(as.character(value[available]), quote)
      }
    }
    
  }
  
  structure(out, class = c("sq_value", "character"))
}

# calling_env -------------------------------------------------------------

calling_env <- function()
{
  top        <- topenv(environment(calling_env))
  frames     <- c(.GlobalEnv, sys.frames())
  topenvs    <- lapply(frames, topenv)
  is_squr    <-
    vapply(topenvs, function(e)
      identical(e, top), logical(1))
  first_squr <- min(which(is_squr))
  
  frames[[first_squr]]
}

# is_packaged -------------------------------------------------------------

is_packaged <- function()
{
  exists(".packageName",
         calling_env(),
         mode = "character",
         inherits = TRUE)
}

package_name <- function()
{
  tryCatch(
    get(
      ".packageName",
      envir    = calling_env(),
      mode     = "character",
      inherits = TRUE
    ),
    error = function(e)
      character(0)
  )
}

# read_sql_file -----------------------------------------------------------

read_sql_file <- function(path, remove_ignored = TRUE)
{
  content <- paste(readLines(path, warn = FALSE), collapse = "\n")
  
  if (isTRUE(remove_ignored))
    gsub("^(?:[\t ]*(?:\r?\n|\r))+",
         "",
         gsub("--rignore.*?--end", "", content),
         perl = TRUE)
  else
    content
  
}


# sq_text -----------------------------------------------------------------

sq_text <- function(text)
{
  if (!is_scalar_character(text))
    stop("Argument 'text' should be a scalar character value")
  
  structure(text, class = c("sq", "sql", "character"))
}


# sq_file -----------------------------------------------------------------

sq_file <- function(path)
{
  if (!is_scalar_character(path))
    stop("Argument 'path' should be a scalar character value")
  
  path.sql <- append_sql_extension(path)
  
  
  if (is_packaged()) {
    pkg_name <- package_name()
    use_path <- system.file(path.sql, package = pkg_name)
    if (use_path == "")
      stop(sprintf(
        "The SQL file '%s' cannot be found in package '%s'",
        path.sql,
        pkg_name
      ))
    
  } else {
    use_path <- path.sql
    
  }
  
  normalized <- normalizePath(use_path, mustWork = FALSE)
  
  if (!file.exists(normalized))
    stop(sprintf("The SQL file '%s' cannot be found.", normalized))
  
  sql <- read_sql_file(normalized)
  
  sq_text(sql)
}

# sq_set ------------------------------------------------------------------

sq_set <- function(.query, ...)
{
  params <- lapply(list(...), sq_value)
  names <- names(params)
  
  if (is.null(names) || any(names == ""))
    stop("Parameters must be named.")
  
  sort_index <- order(nchar(names))
  
  sq_set_(.query, params[sort_index])
}

sq_set_ <- function(query, params)
{
  param <- names(params)[[1L]]
  value <- params[[1L]]
  
  pattern <- paste0(param, "(?![[:alnum:]_#\\$\\@:])")
  
  prefix  <-
    `if`(any(grepl(paste0("@_", pattern), query, perl = TRUE)), "@_", "@")
  
  result <- gsub(paste0(prefix, pattern), value, query, perl = TRUE)
  
  if (length(params) > 1)
    sq_set_(result, params[-1])
  else
    sq_text(result)
}

