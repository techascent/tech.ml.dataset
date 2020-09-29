#!/bin/bash

DATA_DIR=test/data/ames-house-prices

mkdir -p $DATA_DIR

wget https://s3.us-east-2.amazonaws.com/tech.public.data/house-prices-advanced-regression-techniques.zip

unzip -o house-prices-advanced-regression-techniques.zip -d $DATA_DIR

# Of course the files have incorrect permissions...

chmod 644 $(find test/data/ames-house-prices -type f)

rm house-prices-advanced-regression-techniques.zip
