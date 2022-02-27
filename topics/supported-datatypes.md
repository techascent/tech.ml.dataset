# tech.ml.dataset Supported Datatypes


`tech.ml.dataset` supports a wide range of datatypes and has a system for expanding
the supported datatype set, aliasing new names to existing datatypes, and packing
object datatypes into primitive containers.  Let's walk through each of these topics
and finally see how they relate to actually getting data into and out of a dataset.


## Typesystem Fundamentals


### Base Concepts


There are two fundamental namespaces that describe the entire type system for
`dtype-next` derived projects.  The first is the [casting
namespace](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/casting.clj)
- this registers the various datatypes and has maps describing the current set of
datatypes.  `dtype-next` has a simple typesystem in order to support primitve
unsigned types which are completely unsupported on the JVM otherwise.


If we just load the casting namespace we see the base dtype-next datatypes:


```clojure
user> (require '[tech.v3.datatype.casting :as casting])
nil
user> @casting/valid-datatype-set
#{:byte :int8 :float32 :long :bool :int32 :int :object :float64 :string :uint64 :uint16 :boolean :short :double :char :keyword :uint8 :uuid :uint32 :int16 :float :int64}
```

Now if we load the dtype-next namespace we see quite a few more datatypes registered:


```clojure
user> (require '[tech.v3.datatype :as dtype])
nil
user> @casting/valid-datatype-set
#{:byte :int8 :float32 :char-array :int :object-array :float64 :list :uint64 :uint16 :char :int64-array :uint8 :int32-array :boolean-array :persistent-map :persistent-vector :persistent-set :float :long :bool :int32 :object :int16-array :string :boolean :short :float64-array :double :float32-array :keyword :uuid :int8-array :native-buffer :uint32 :array-buffer :int16 :int64}
```

Right away you can perhaps tell that there is a dynamic mechanism for registering more datatypes
 - we will get to that later.  This set ties into the dtype-next [datatype api](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-datatype):


```clojure
user> (dtype/datatype (java.util.UUID/randomUUID))
:uuid
user> (dtype/datatype (int 10))
:int32
user> (dtype/datatype (float 10))
:float32
user> (dtype/datatype (double 10))
:float64
```


If we have a container of data one important question we have is what type of data is in
the container.  This is where the [elemwise-datatype api](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-elemwise-datatype) comes in:


```clojure
user> (dtype/elemwise-datatype (float-array 10))
:float32
user> (dtype/elemwise-datatype (int-array 10))
:int32
```

Given 2 (or more) numeric datatypes we can ask the typesystem what datatype should a combined
operation, such as `+`, operate in?

```clojure
user> (casting/widest-datatype :float32 :int32)
:float64
```

The root of our type system is the object datatype.  All types can be represented by the object
datatype albeit at some cost and generic containers such as persistent vectors or java
`ArrayList`s and generic sequences produced by operations such as `map` do not have any
information about the type of data they contain and thus they have the dataytpe of `:object`:

```clojure
user> (dtype/elemwise-datatype (range 10))
:int64
user> (dtype/elemwise-datatype (vec (range 10)))
:object
```

If we include the dataset api then we see the typesystem is extended to include support for
various datetime types:

```clojure
user> (require '[tech.v3.dataset :as ds])
nil
user> @casting/valid-datatype-set
#{:byte :int8 :local-date-time :float32 :char-array :int :object-array :epoch-milliseconds :uint64 :char :packed-instant :uint8 :bitmap :int32-array :boolean-array :persistent-map :persistent-vector :days :tensor :persistent-set :seconds :long :microseconds :int32 :boolean :short :double :epoch-days :float32-array :instant :zoned-date-time :keyword :dataset :text :native-buffer :array-buffer :years :int64 :epoch-microseconds :milliseconds :float64 :list :uint16 :int64-array :nanoseconds :duration :packed-duration :float :bool :object :int16-array :string :hours :float64-array :epoch-seconds :packed-local-date :epoch-hours :uuid :weeks :local-date :int8-array :uint32 :int16}
```


Given a container of a with a specific datatype we can create a new read-only representation
of a datatype that we desire with [make-reader](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-make-reader):


```clojure
user> (def generic-data (vec (range 10)))
#'user/generic-data
user> generic-data
[0 1 2 3 4 5 6 7 8 9]
user> (dtype/make-reader :float32 (count generic-data) (float (generic-data idx)))
[0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0]
```

The default datetime definition of all datatypes is in [datatype/base.clj](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/datetime/base.clj#L398).


### Packing


The second fundamental concept to the typesystem is the concept of packing which is storing
a java object in a primitive datatype.  This allows us to use `:int64` data to represent
`java.time.Instant` objects and `:int32` data to represent `java.time.LocalDate` objects.
This compression has both speed and size benefits especially when it comes to serializing
the data.  It also allows us to support parquet and apache arrow file formats more
transparently because they represent, e.g. `LocalDate` objects as epoch days.  Currently
only datetime objects are packed.


Packing has generic support in the underlying buffer system so that it works in an integrated
fashion throughout the system.

```clojure
user> (dtype/make-container :packed-local-date (repeat 10 (java.time.LocalDate/now)))
#array-buffer<packed-local-date>[10]
[2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15]
user> (def packed *1)
#'user/packed
user> (def unpacked (dtype/make-container :local-date (repeat 10 (java.time.LocalDate/now))))
#'user/unpacked
user> unpacked
#array-buffer<local-date>[10]
[2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15, 2021-12-15]
user> (.readLong (dtype/->reader packed) 0)
18976
user> (.readObject (dtype/->reader packed) 0)
#object[java.time.LocalDate 0x2d867250 "2021-12-15"]
user> (.toEpochDay *1)
18976
user> (.readLong (dtype/->reader unpacked) 0)
Execution error at tech.v3.datatype.NumericConversions/numberCast (NumericConversions.java:22).
Invalid argument
user> (.readObject (dtype/->reader unpacked) 0)
#object[java.time.LocalDate 0x2f13a18c "2021-12-15"]
```

Packing is defined in the namespace [tech.v3.datatype.packing](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/packing.clj).  We can add new packed datatypes
but I strongly suggest avoiding this in general.  While it certainly works well it is usually
unnecessary and less clear than simply defining an alias and conversion methods do/from
the alias.


The best example of using the packing system is the definition of the [datetime packed
datatypes](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/datetime/packing.clj).


### Aliasing Datatypes


C/C++ contain the concept of datatype aliasing in the `typedef` keyword.  For our use cases
it is useful, especially when dealiing with datetime types to alias some datatypes to integers
of various sizes so you can have a container of `:milliseconds` and such.  You can see several
examples in the aforementioned [datatime/base.clj](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/datetime/base.clj#L398).


```clojure
user> (casting/alias-datatype! :foobar :float32)
#{:byte :int8 :local-date-time :float32 :char-array :int :object-array :epoch-milliseconds :uint64 :char :packed-instant :uint8 :bitmap :int32-array :boolean-array :persistent-map :persistent-vector :days :tensor :persistent-set :seconds :long :microseconds :int32 :boolean :short :double :epoch-days :float32-array :instant :zoned-date-time :keyword :dataset :text :native-buffer :array-buffer :years :int64 :epoch-microseconds :milliseconds :float64 :list :uint16 :int64-array :nanoseconds :duration :packed-duration :float :bool :foobar :object :int16-array :string :hours :float64-array :epoch-seconds :packed-local-date :epoch-hours :uuid :weeks :local-date :int8-array :uint32 :int16}
user> (dtype/make-container :foobar (range 10))
#array-buffer<foobar>[10]
[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000]
```


In general, because this is all done at runtime I ask that people refrain aliasing new
datatypes, defining new datatypes, and packing new datatypes.  This doesn't mean it is an
error if someone does it but it does mean that every new datatype definition, packing
definition, and alias definition slightly slows down the system.



## Supported Meaningful Datatypes


For dataset processing, the currently supported meaningful datatypes are:

* `[:int8 :uint8 :int16 :uint16 :int32 :uint32 :int64 :uint64 :float32 :float64
    :string :keyword :uuid
	:local-date :packed-local-date :instant :packed-instant :duration :packed-duration
	:local-date-time]`


There are more datatypes but for general purpose dataset processing these are a reasonable
subset.


When parsing data into the dataset system we can define both the container of the data
and the parser of the data:


```clojure
user> (def data-maps (for [idx (range 10)]
                           {:a idx
                            :b (str (.plusDays (java.time.LocalDate/now) idx))}))
#'user/data-maps
user> data-maps
({:a 0, :b "2021-12-15"}
 {:a 1, :b "2021-12-16"}
 {:a 2, :b "2021-12-17"}
 {:a 3, :b "2021-12-18"}
 {:a 4, :b "2021-12-19"}
 {:a 5, :b "2021-12-20"}
 {:a 6, :b "2021-12-21"}
 {:a 7, :b "2021-12-22"}
 {:a 8, :b "2021-12-23"}
 {:a 9, :b "2021-12-24"})
user> (:b (ds/->dataset data-maps))
#tech.v3.dataset.column<string>[10]
:b
[2021-12-15, 2021-12-16, 2021-12-17, 2021-12-18, 2021-12-19, 2021-12-20, 2021-12-21, 2021-12-22, 2021-12-23, 2021-12-24]
user> (:b (ds/->dataset data-maps {:parser-fn {:b :local-date}}))
#tech.v3.dataset.column<local-date>[10]
:b
[2021-12-15, 2021-12-16, 2021-12-17, 2021-12-18, 2021-12-19, 2021-12-20, 2021-12-21, 2021-12-22, 2021-12-23, 2021-12-24]
user> (:b (ds/->dataset data-maps {:parser-fn {:b :packed-local-date}}))
#tech.v3.dataset.column<packed-local-date>[10]
:b
[2021-12-15, 2021-12-16, 2021-12-17, 2021-12-18, 2021-12-19, 2021-12-20, 2021-12-21, 2021-12-22, 2021-12-23, 2021-12-24]
user> (:b (ds/->dataset data-maps {:parser-fn {:b [:packed-local-date
                                                   (fn [data]
                                                     (java.time.LocalDate/parse (str data)))]}}))
#tech.v3.dataset.column<packed-local-date>[10]
:b
[2021-12-15, 2021-12-16, 2021-12-17, 2021-12-18, 2021-12-19, 2021-12-20, 2021-12-21, 2021-12-22, 2021-12-23, 2021-12-24]
```


## Extending Datatype System


Lets say we have tons of data in which only year-months are relevant.  We can have
escalating levels of support depending on how much it really matters.  The first level is to
convert the data into a type the system already understands, in this case a LocalDate:


```clojure
user> (:b (ds/->dataset
           data-maps
           {:parser-fn {:b [:packed-local-date
                            (fn [data]
                              (let [ym (java.time.YearMonth/parse (str data))]
                                (java.time.LocalDate/of (.getYear ym) (.getMonth ym) 1)))]
                              }}))
#tech.v3.dataset.column<packed-local-date>[10]
:b
[2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01, 2021-12-01]
```


The second is to parse to year-months and accept our column type will just be `:object` -


```clojure
user> (:b (ds/->dataset
           data-maps
           {:parser-fn {:b [:object
                            (fn [data]
                              (let [ym (java.time.YearMonth/parse (str data))]
                                ym))]
                              }}))
#tech.v3.dataset.column<object>[10]
:b
[2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12]
```

Third, we can extend the type system to support year month's as object datatypes.  This is
only slightly better than base object support but it does allow us to ensure we can
make containers with only `YearMonth` or nil objects in them:


```clojure
user> (casting/add-object-datatype! :year-month java.time.YearMonth)
:ok
user> (:b (ds/->dataset
           data-maps
           {:parser-fn {:b [:year-month
                            (fn [data]
                              (let [ym (java.time.YearMonth/parse (str data))]
                                ym))]
                              }}))
#tech.v3.dataset.column<year-month>[10]
:b
[2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12, 2021-12]
```

And finally we could implement packing for this type.  This means we could store year-month as
perhaps 32-bit integer epoch-months or something like that.  We won't demonstrate this as
it is tedious but the example in [datetime/packing.clj](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/datetime/packing.clj) 
may be sufficient to show how to do this - if not let us know on Zulip or drop me an email.
