# tech.ml.dataset

![TMD Logo](logo.png "TMD")

`tech.ml.dataset` (TMD) is a Clojure library for tabular data processing similar to Python's Pandas, or R's `data.table`. It supports pragmatic data-intensive work on the JVM by providing powerful abstractions that simplify implementing efficient solutions to real problems. Datasets [shrink in memory](https://gist.github.com/cnuernber/26b88ed259dd1d0dc6ac2aa138eecf37) through columnar storage and the use of primitive arrays, packed datetime types, and string tables.

Unlike in Python or R, TMD datasets are _functional_, which means they're easier to reason about.

## Installing

Installation instructions for your favorite build system (lein, deps.edn, etc...) can be found at Clojars, where the library is hosted:

[![Clojars Project](https://img.shields.io/clojars/v/techascent/tech.ml.dataset.svg)](https://clojars.org/techascent/tech.ml.dataset)

 - [https://clojars.org/techascent/tech.ml.dataset](https://clojars.org/techascent/tech.ml.dataset)

## Verifying Installation

```clojure
user> (require 'tech.v3.dataset)
nil
user> (->> (System/getProperties)
           (map (fn [[k v]] {:k k :v (apply str (take 40 (str v)))}))
           (tech.v3.dataset/->>dataset {:dataset-name "My Truncated System Properties"}))

My Truncated System Properties [53 2]:

|                         :k |                                       :v |
|----------------------------|------------------------------------------|
|                sun.desktop |                                    gnome |
|                awt.toolkit |                     sun.awt.X11.XToolkit |
| java.specification.version |                                       11 |
|            sun.cpu.isalist |                                          |
|           sun.jnu.encoding |                                    UTF-8 |
|            java.class.path | src:resources:target/classes:/home/harol |
|             java.vm.vendor |                                   Ubuntu |
|        sun.arch.data.model |                                       64 |
|            java.vendor.url |                      https://ubuntu.com/ |
|              user.timezone |                           America/Denver |
|                        ... |                                      ... |
|                    os.arch |                                    amd64 |
| java.vm.specification.name |       Java Virtual Machine Specification |
|        java.awt.printerjob |                   sun.print.PSPrinterJob |
|         sun.os.patch.level |                                  unknown |
|          java.library.path | /usr/java/packages/lib:/usr/lib/x86_64-l |
|               java.vm.info |                      mixed mode, sharing |
|                java.vendor |                                   Ubuntu |
|            java.vm.version |      11.0.17+8-post-Ubuntu-1ubuntu222.04 |
|    sun.io.unicode.encoding |                            UnicodeLittle |
|        apple.awt.UIElement |                                     true |
|         java.class.version |                                     55.0 |
```

## 📚 Documentation 📚

The best place to start is the "Getting Started" topic in the documentation: [https://techascent.github.io/tech.ml.dataset/000-getting-started.html](https://techascent.github.io/tech.ml.dataset/000-getting-started.html)

The "Walkthrough" topic provides long-form examples of processing real data: [https://techascent.github.io/tech.ml.dataset/100-walkthrough.html](https://techascent.github.io/tech.ml.dataset/100-walkthrough.html)

The "Quick Reference" topic summarizes many of the most frequently used functions: [https://techascent.github.io/tech.ml.dataset/200-quick-reference.html](https://techascent.github.io/tech.ml.dataset/200-quick-reference.html)

The API docs document every available function: [https://techascent.github.io/tech.ml.dataset/](https://techascent.github.io/tech.ml.dataset/)

The provided Java API ([javadoc](https://techascent.github.io/tech.ml.dataset/javadoc/tech/v3/TMD.html) / [with frames](https://techascent.github.io/tech.ml.dataset/javadoc/index.html)) and sample program ([source](java_test/java/jtest/TMDDemo.java)) show how to use TMD from Java.

## Questions / Community

* Log an [issue](https://github.com/techascent/tech.ml.dataset/issues)!
* Visit the [zulip stream](https://clojurians.zulipchat.com/#narrow/stream/236259-tech.2Eml.2Edataset.2Edev).
* Or the [slack data science channel](https://clojurians.slack.com/archives/C0BQDEJ8M).

-----

### Related Projects and Notes

* An alternative cutting-edge api with some important extra features is available via [tablecloth](https://github.com/scicloj/tablecloth).
* [tech.v3.datatype](https://github.com/cnuernber/dtype-next) provides the underlying numeric subsystem to TMD.
* Simple regression/classification machine learning pathways are available in [tech.ml](https://github.com/techascent/tech.ml).
* Some [independent benchmarks](https://github.com/zero-one-group/geni-performance-benchmark/) indicating TMD's _speed_.
* A Graal native [example project](https://github.com/cnuernber/ds-graal).
* The [scicloj.ml tutorials](https://github.com/scicloj/scicloj.ml-tutorials) may be a good way to jump straight into data science.
* [Comparison](https://github.com/genmeblog/techtest/blob/master/src/techtest/datatable_dplyr.clj) between R's `data.table`, R's `dplyr`, and an older version of TMD.
* Another overview of some of the available functions from genme: [Some Functions](https://github.com/genmeblog/techtest/wiki/Summary-of-functions)

### License

Copyright © 2023 Complements of TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
