# Pandas

The gorilla in the room for very good reason.  Pandas is amazing and well built especially for the cases where
the dataset is very large.

We found a great slide deck on the 
[design and implementation](http://www.jeffreytratner.com/slides/pandas-under-the-hood-pydata-seattle-2015.pdf) 
of pandas.


## High Level Similarities

* 'data-frame' - column store db with heterogeneous column types including string and
  object column types.
* Designed to make the majority of data munging fast and easy.
* Missing values are first class things that you can do something about.


## High Level Differences

* Pandas supports very large datasets and has a significant portion written in C.  It
uses block storage under the hood for this type of support meaning it may support
datasets as large as can fit on a drive with some significant swapping.

* Currently tech.ml.dataset is purely based on tablesaw and purely in-memory.  It has no
knowledge of blocks or swapping although an easy extension would be to extend it to
support a logical sequence of tablesaw datasets but keeping things like string tables in
memory.

* Pandas is meant for immediate mode imperative performance.  Here is the largest
important difference.  The tech.ml.dataset abstraction is meant to run during training
and produce a new pipeline with learned values (means, medians, string tables, etc)
encoded into the new pipeline.  Then during inference later you can use the encoded
pipeline *without* having to recreate a subset of you your training process.

* The JVM is about 100 times faster than python.  So some number of custom
transformations are possible in the JVM whereas in python you *have* to drop down into
c-python.  This enables a much larger surface area of custom behavior and a cheaper
means of implementing and maintaining it.


## Takeaway

Pandas is industrial strength and tested.  If you have extremely large datasets (greater
than will fit in RAM) then you have your answer.  The tech system is new, prototypical,
and in memory.  If you want to do pandas type things on the JVM right now and help move
it from prototypical to tested (a large jump) or if you have small to medium sized
datasets, then it will work fine.
