# Parsing Difficult Files

The biggest blocking to dealing with a lot of datasets is just getting them
to parse.  Tablesaw has an autodetection mechanism but it is important
to deal with situations where that detection fails or is insufficient.

## Separator

We normally auto-detect the separator by scanning the first 1000 bytes or
so and counting the number of ',' vs 'tab'.  This works decent but you
may need to override this with `:separator`.


## Column Datatype

Here is where the difficult stuff comes in.  We load the first 64K of data
and attempt to use that to detect the rest using tablesaw's extended column
types.  This may fail so there are two levers you may need:


* `:autodetect-max-bytes` - If you think scanning more than 64K of data will
solve your problem, by all means increase this number.
* `:column-type-fn` - This is the big stick.  It gets passed the file data including
the header row just after parsing via clojure.data.csv.  So it gets passed a sequence
containing the sequences parsed out of the csv file.  It should return a sequence
of keywords indicating the column types desired for the data; the keywords are
a subset of datatypes of 'tech.datatype': `:int16`, `:int32`, `:int64`, `:float32`,
`:float64`, `:boolean`, `:string`.


Those are the levers.  You can tell the system to scan more data and you can do your
own autodetection if you need to.
