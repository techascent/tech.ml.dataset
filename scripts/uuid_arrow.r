#!/usr/bin/Rscript

library(dplyr)
library(arrow)
library(uuid)

df=data_frame(uuids=rep(UUIDgenerate(), 10))

arrow::write_ipc_stream(df, "test/data/uuid.arrow")