#! /usr/bin/env Rscript

# Setup sink so only JSON is returned in stdout
sink('stdout', type=c("output", "message"))

# load libraries
suppressPackageStartupMessages(library("jsonlite"))
suppressWarnings(library("SKAT"))

# read json data from stdin
args <- commandArgs(trailingOnly = TRUE)
json_data <- fromJSON(file("stdin"), simplifyVector=TRUE, simplifyMatrix=TRUE)

# get phenotype vector
Y <- json_data$Y

# calculate null model
# FIXME: add continuous phenotype support
if ("COV" %in% names(json_data)) {
   obj <- SKAT_Null_Model(Y ~ json_data$COV, out_type="D")
} else {
   obj <- suppressWarnings(SKAT_Null_Model(Y ~ 1, out_type="D"))
}

group_data <- json_data$groups

runSkatO <- function(name, data) {
	 Z <- t(data)
	 suppressWarnings(res <- SKAT(Z, obj))
	 return(list(groupName = unbox(name), 
	             pValue = unbox(res$p.value), 
	             pValueNoAdj = unbox(res$p.value.noadj), 
	             nMarker = unbox(res$param$n.marker), 
	             nMarkerTest = unbox(res$param$n.marker.test)
	             )
	        )
}

group_data <- json_data$groups
groupNames <- names(group_data)
results <- lapply(groupNames, function(k) runSkatO(k, group_data[[k]]))

# remove sink for stdout
sink()

# output json with results to stdout
cat(minify(toJSON(results, digits = I(4), null = "null", na = "null")))
