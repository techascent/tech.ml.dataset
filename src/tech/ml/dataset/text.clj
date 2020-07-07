(ns tech.ml.dataset.text
  (:require [tech.ml.dataset.string-table :as str-table]
            [clojure.string :as str]
            [tech.v2.datatype.protocols :as dtype-proto]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.casting :as casting]
            [tech.v2.datatype.object-datatypes :as obj-dtypes]
            [taoensso.nippy :as nippy])
  (:import [java.util List ArrayList Collection]
           [java.nio.charset StandardCharsets Charset]
           [java.util Arrays]
           [it.unimi.dsi.fastutil.bytes ByteBigArrayBigList ByteArrayList]
           [it.unimi.dsi.fastutil.longs LongArrayList]))


(set! *warn-on-reflection* true)


(def default-tokenizer #(str/split % #"[ ]+"))
(def default-charset StandardCharsets/UTF_8)


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


(deftype EncodedTextBuilder [encode-fn
                             decode-fn
                             ^ByteBigArrayBigList backing-store
                             ^LongArrayList offsets
                             encoding]
  List
  (size [this] (.size offsets))
  (add [this item] (.add this (.size this) item) true)
  (add [this idx item]
    (.add offsets (.size64 backing-store))
    (let [^bytes data (encode-fn item)]
      (.addAll backing-store (ByteArrayList. data))))
  (addAll [this coll]
    (doseq [item coll]
      (.add this item)))
  (get [this idx]
    (let [offset (.get offsets idx)
          next-idx (unchecked-inc idx)
          end-off (long (if (< next-idx (.size offsets))
                          (.get offsets next-idx)
                          (.size64 backing-store)))]
      (decode-fn (-> (.subList backing-store offset end-off)
                     (.toByteArray)))))
  (set [this idx value]
    (throw (Exception. "Cannot set string value in encoded text builder")))
  (toArray [this]
    (dtype/make-container :java-array :object this))
  (subList [this start-idx end-idx]
    (let [n-elems (int (- end-idx start-idx))]
      (reify
        dtype-proto/PDatatype
        (get-datatype [subl] :encoded-text)
        List
        (size [subl] n-elems)
        (get [subl idx]
          (.get this (+ start-idx idx)))
        (toArray [this]
          (dtype/make-container :java-array :object this))
        dtype-proto/PDatatype
        (get-datatype [this] :string)
        dtype-proto/PToReader
        (convertible-to-reader? [this] true)
        (->reader [this options]
          (->
           (reify tech.v2.datatype.ObjectReader
             (getDatatype [rdr] :string)
             (lsize [rdr] n-elems)
             (read [rdr idx] (.get this (+ start-idx idx))))
           (dtype-proto/->reader options)))
        dtype-proto/PClone
        (clone [this] this))))
  (iterator [this]
    (.iterator ^Iterable (dtype-proto/->reader this {})))
  dtype-proto/PDatatype
  (get-datatype [this] :encoded-text)
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (-> (reify tech.v2.datatype.ObjectReader
          (getDatatype [rdr] :string)
          (lsize [rdr] (long (.size offsets)))
          (read [rdr idx]
            (.get this idx)))
        (dtype-proto/->reader options)))
  dtype-proto/PClone
  (clone [this]
    ;;Clone to exactly the size we need
    (EncodedTextBuilder. encode-fn decode-fn
                         (.clone backing-store)
                         (.clone offsets)
                         encoding)))


(casting/alias-datatype! :encoded-text :string)


(defprotocol PEncodingToFn
  (encoding->encode-fn [encoding])
  (encoding->decode-fn [encoding]))


(defrecord Encoding [encoding-name]
  PEncodingToFn
  (encoding->encode-fn [enc]
    (let [charset (Charset/forName encoding-name)]
      #(.getBytes ^String % charset)))
  (encoding->decode-fn [enc]
    (let [charset (Charset/forName encoding-name)]
      #(String. ^bytes % charset))))


(defn- check-encoding
  [str-enc]
  (boolean (Charset/forName str-enc)))


(defn encoded-text-builder
  (^List [encoding]
   (let [encoding (cond
                    (instance? Charset encoding)
                    (->Encoding (.toString ^Object encoding))
                    (string? encoding)
                    (do
                      (check-encoding encoding)
                      (->Encoding encoding))
                    :else
                    encoding)]
     (EncodedTextBuilder. (encoding->encode-fn encoding)
                          (encoding->decode-fn encoding)
                          (ByteBigArrayBigList.)
                          (LongArrayList.)
                          encoding)))
  (^List []
   (encoded-text-builder default-charset)))

(defn enc-builder->data
  [^EncodedTextBuilder enc]
  {:backing-store (.elements
                   ^ByteBigArrayBigList
                   (dtype/clone
                    (.backing-store enc)))
   :offsets (dtype/->array-copy (.offsets enc))
   :encoding (.encoding enc)})


(defn data->enc-builder
  [data]
  (let [{:keys [backing-store offsets encoding]} data]
    (EncodedTextBuilder. (encoding->encode-fn encoding)
                        (encoding->decode-fn encoding)
                        (ByteBigArrayBigList/wrap backing-store)
                        (LongArrayList/wrap offsets)
                        encoding)))


(nippy/extend-freeze
 EncodedTextBuilder :tech.ml.dataset.text/encoded-text
 [^EncodedTextBuilder enc out]
 (nippy/-freeze-without-meta! (enc-builder->data enc) out))


(nippy/extend-thaw
 :tech.ml.dataset.text/encoded-text
 [in]
 (-> (nippy/thaw-from-in! in)
     (data->enc-builder)))
