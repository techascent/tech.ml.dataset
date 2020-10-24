#!/usr/bin/Rscript

library(arrow)
library(dplyr)

df=data_frame(texts=rep("A character vector containing abbreviations for the character strings in its first argument. Duplicates in the original names.arg will be given identical abbreviations. If any non-duplicated elements have the same minlength abbreviations then, if method = both.sides the basic internal abbreviate() algorithm is applied to the characterwise reversed strings; if there are still duplicated abbreviations and if strict = FALSE as by default, minlength is incremented by one and new abbreviations are found for those elements only. This process is repeated until all unique elements of names.arg have unique abbreviations.",10000000)
)
arrow::write_ipc_stream(df,"10m.arrow")