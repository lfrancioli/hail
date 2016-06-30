#! /usr/bin/env Rscript

# Setup sink so only JSON is returned in stdout
#sink('stdout', type=c("output", "message"))

# load libraries
suppressWarnings(library("SKAT"))

# read json data from stdin
args <- commandArgs(trailingOnly = TRUE)

plinkRoot <- args[1]
covar <- args[2]
setID <- args[3]
phenoType <- args[4]
ssdFile <- args[5]
infoFile <- args[6]
resultsFile <- args[7]

Generate_SSD_SetID(paste(plinkRoot, ".bed", sep=""), 
                   paste(plinkRoot, ".bim", sep=""),
                   paste(plinkRoot, ".fam", sep=""),
                   setID,
                   ssdFile,
                   infoFile)

isBinary <- TRUE
if (phenoType == "C") {
  isBinary <-FALSE
}

FAM <- Read_Plink_FAM(paste(plinkRoot, ".fam", sep=""), Is.binary=isBinary)
Y <- FAM$Phenotype
print(length(which(!is.na(Y))))
SSD.INFO <- Open_SSD(ssdFile, infoFile)

print(phenoType)
obj <- SKAT_Null_Model(Y ~ 1, out_type=phenoType)

out <- SKAT.SSD.All(SSD.INFO, obj)
Close_SSD()
#sink()
write.table(out$results, resultsFile, row.names = FALSE, col.names = FALSE, quote = FALSE)
warnings()




