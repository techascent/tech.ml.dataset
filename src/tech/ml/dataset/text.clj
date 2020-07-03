(ns tech.ml.dataset.text
  (:require [tech.ml.dataset.string-table :as str-table]
            [clojure.string :as str]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.object-datatypes :as obj-dtypes])
  (:import [java.util List ArrayList Collection]
           [java.nio.charset StandardCharsets Charset]
           [java.util Arrays]))


(set! *warn-on-reflection* true)


(def default-tokenizer #(str/split % #"[ ]+"))


(deftype TokenizedText [str-table options ^long offset ^long len]
  dtype-proto/PCountable
  (ecount [this] len)
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (let [options (if (= (:datatype options) :text)
                    (assoc options :datatype :string)
                    options)]
      (dtype-proto/->reader
       (dtype/sub-buffer str-table offset len)
       options)))
  dtype-proto/PBuffer
  (sub-buffer [this off len]
    (TokenizedText. str-table options (+ offset (long off)) (long len)))
  ;;This is a functional datastructure so clone can return the this object.
  dtype-proto/PClone
  (clone [this] this)
  Object
  (toString [this]
    (str/join (or (:token options)
                  " ")
              (dtype/sub-buffer str-table offset len))))


(defn string->tokenized-text!
  "Mutates the string table adding tokens and records offset and length in a text
  object."
  (^TokenizedText [str-table {:keys [token tokenizer]
                              :or {token " "
                                   tokenizer default-tokenizer}}
                   str-data]
   (let [tokens (tokenizer str-data)
         offset (dtype/ecount str-data)
         len (dtype/ecount tokens)
         ^List str-table str-table]
     (doseq [token tokens]
       (.add str-table token))
     (TokenizedText. str-table token offset len)))
  (^TokenizedText [str-table str-data]
   (string->tokenized-text! str-table {} str-data))
  (^TokenizedText [str-data]
   (string->tokenized-text! (str-table/make-string-table) {} str-data)))


(defn- construct-tokenized-text
  (^TokenizedText []
   (string->tokenized-text! ""))
  (^TokenizedText [item]
   (cond
     (instance? TokenizedText item)
     item
     (string? item)
     (string->tokenized-text! item)
     (nil? item)
     nil
     :else
     (throw (Exception.
             (format "Unable to construct text object from type: %s"
                     (type item)))))))


(obj-dtypes/add-object-datatype TokenizedText :tokenized-text
                                construct-tokenized-text)


;;Used during parsing.  Only implements add functionality
(deftype TokenizedTextBuilder [str-table options ^List data-list]
  List
  (size [this] (.size data-list))
  (add [this item]
    (.add this (.size this) item)
    true)
  (add [this idx item]
    (.add data-list idx (string->tokenized-text! str-table options item)))
  (addAll [this coll]
    (doseq [item coll]
      (.add this item)))
  (get [this idx]
    (.get data-list idx))
  (set [this idx value]
    (locking str-table
      (let [text-obj
            (cond
              (string? value)
              (string->tokenized-text! str-table options value)
              (instance? TokenizedText value)
              value
              (nil? value)
              (string->tokenized-text! str-table options "")
              :else
              (throw (Exception.
                      (format "Cannot add non string/text value to text builder: %s"
                              (type value)))))]
        (.set data-list idx text-obj))))
  (subList [this start-idx end-idx]
    (TokenizedTextBuilder. str-table options (.subList data-list start-idx end-idx)))
  (toArray [this]
    (.toArray data-list))
  (iterator [this]
    (.iterator data-list))
  dtype-proto/PDatatype
  (get-datatype [this] :string)
  dtype-proto/PClone
  (clone [this]
    (.toArray this)))


(defn make-tokenized-text-builder
  (^List [str-table options]
   (TokenizedTextBuilder. str-table options (ArrayList.)))
  (^List [str-table]
   (make-tokenized-text-builder str-table {}))
  (^List []
   (make-tokenized-text-builder (str-table/make-string-table) {})))


(deftype EncodedText [^Charset encoding ^bytes data
                      ^{:volatile-mutable true} hashcode]
  Object
  (hashCode [this]
    (let [retval (unchecked-int hashcode)]
      (if (== retval -1)
        (do
          (set! hashcode (.hashCode (.toString this)))
          (unchecked-int hashcode))
        retval)))
  (toString [this]
    (.toString (.decode encoding data)))
  (equals [this other]
    (if-not (instance? EncodedText other)
      false
      (let [^EncodedText other other]
        (and (= encoding (.encoding other))
             (Arrays/equals data ^bytes (.data other)))))))


(def default-charset StandardCharsets/UTF_8)


(defn encode-text
  (^EncodedText []
   (encode-text "" default-charset))
  (^EncodedText [item]
   (cond
     (instance? EncodedText item)
     item
     (string? item)
     (encode-text item default-charset)
     (nil? item)
     nil
     :else
     (throw (Exception.
             (format "Unable to encode text type: %s"
                     (type item))))))
  (^EncodedText [data encoding]
   (let [data (or data "")]
     (when-not (instance? Charset encoding)
       (throw (Exception. "Encoding is not a charset: %s"
                          encoding)))
     (when-not (string? data)
       (throw (Exception. (format "data must be a string: %s"
                                  data))))
     (let [^Charset encoding encoding]
       (EncodedText. encoding (.encode encoding ^String data) (int -1))))))


(obj-dtypes/add-object-datatype EncodedText :encoded-text encode-text)


(defn- as-collection ^Collection [item] item)


(deftype EncodedTextBuilder [encoding ^List data-list]
  List
  (size [this] (.size data-list))
  (add [this item] (.add this (.size this) item) true)
  (add [this idx item] (.add data-list idx (encode-text item encoding)))
  (addAll [this coll]
    (->> coll
         (map #(encode-text encoding %))
         (as-collection)
         (.addAll data-list)))
  (get [this idx] (.get data-list idx))
  (set [this idx value] (.set data-list idx (encode-text value encoding)))
  (toArray [this] (.toArray data-list))
  (subList [this start-idx end-idx]
    (EncodedTextBuilder. encoding (.subList data-list start-idx end-idx)))
  dtype-proto/PDatatype
  (get-datatype [this] :encoded-text)
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (dtype-proto/->reader data-list options))
  dtype-proto/PClone
  (clone [this]
    (.toArray this)))


(defn encoded-text-builder
  (^List []
   (EncodedTextBuilder. default-charset
                        (dtype/make-container :list :encoded-text 0)))
  (^List [n-elems]
   (EncodedTextBuilder. default-charset
                        (dtype/make-container :list :encoded-text n-elems)))
  (^List [charset n-elems]
   (when-not (instance? Charset charset)
     (throw (Exception.
             (format "charset must be an instance of java.nio.charsets.Charset: %s")
             (type charset))))
   (EncodedTextBuilder. charset
                        (dtype/make-container :list :encoded-text n-elems))))
