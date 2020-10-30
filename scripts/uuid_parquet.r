#!/usr/bin/Rscript

library(dplyr)
library(arrow)
library(uuid)

df=data_frame(uuids=rep(UUIDgenerate(), 10))

write_parquet(df, "uuid.parquet")