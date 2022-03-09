(ns tech.v3.libs.arrow.liblz4
  (:require [tech.v3.datatype :as dt]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.resource :as resource])
  (:import [tech.v3.datatype.ffi Pointer]))


(def ^{:tag 'long} LZ4F_default 0)
(def ^{:tag 'long} LZ4F_max64KB 4)
(def ^{:tag 'long} LZ4F_max256KB 5)
(def ^{:tag 'long} LZ4F_max1MB 6)
(def ^{:tag 'long} LZ4F_max4MB 7)
(def ^{:tag 'long} LZ4F_VERSION 100)
(def ^{:tag 'long} LZ4F_HEADER_SIZE_MAX 19)
(def ^{:tag 'long} LZ4F_blockLinked 0)
(def ^{:tag 'long} LZ4F_blockIndependent 1)
(def ^{:tag 'long} LZ4F_noContentChecksum 0)
(def ^{:tag 'long} LZ4F_contentChecksumEnabled 1)
(def ^{:tag 'long} LZ4F_noBlockChecksum 0)
(def ^{:tag 'long} LZ4F_blockChecksumEnabled 1)
(def ^{:tag 'long} LZ4F_frame 0)
(def ^{:tag 'long} LZ4F_skippableFrame 1)


(dt-ffi/define-library!
  lz4
  '{:LZ4F_getVersion {:rettype :int32
                      :doc "library version"}
    :LZ4F_createDecompressionContext {:rettype :size-t
                                      :argtypes [[dctxPtr :pointer] ;;ptrptr
                                                 [version :int32]]}
    :LZ4F_freeDecompressionContext {:rettype :size-t
                                    :argtypes [[dctx :pointer]]}

    :LZ4F_isError {:rettype :int32
                   :argtypes [[err :size-t]]}
    :LZ4F_getErrorName {:rettype :string
                        :argtypes [[err :size-t]]}
    :LZ4F_getFrameInfo {:rettype :size-t
                        :argtypes [[dctx :pointer] ;;decompression context
                                   [frameInfoPtr :pointer] ;;frameinfo*
                                   [srcBuffer :pointer] ;;void*
                                   [srcSizePtr :pointer]]} ;;size-t*
    :LZ4F_decompress {:rettype :size-t
                      :argtypes [[dctx :pointer]
                                 [dstbuf :pointer]
                                 [dstsize :pointer] ;;size-t*
                                 [srcbuf :pointer]
                                 [srcsize :pointer] ;;size-t*
                                 [decomp-opt :pointer?]]} ;;dOptPtr

    }
  nil
  nil)

;;checked against actual record layout
(def frame-info-def*
  (delay (dt-struct/define-datatype! :lz4-frame-info
           [{:name :block-size-id :datatype :int32}
            {:name :block-mode :datatype :int32}
            {:name :content-checksum :datatype :int32}
            {:name :frame-type :datatype :int32}
            {:name :content-size :datatype :int64}
            {:name :dict-id :datatype :int32}
            {:name :checksum :datatype :int32}])))


(def decompress-options-def*
  (delay (dt-struct/define-datatype! :lz4-decomp-options
           [{:name :stable-dst :datatype :int32}
            {:name :reserved0 :datatype :int32}
            {:name :reserved1 :datatype :int32}
            {:name :reserved2 :datatype :int32}])))


;; Used for graal native pathways
(defn set-library-instance!
  [lib-instance]
  (dt-ffi/library-singleton-set-instance! lz4 lib-instance))


(defn initialize!
  []
  (dt-ffi/library-singleton-set! lz4 "lz4"))


(defn create-decomp-ctx
  []
  (let [retval
        (resource/stack-resource-context
         (let [ptrptr (dt-ffi/make-ptr :pointer 0)
               ctx-err (LZ4F_createDecompressionContext ptrptr LZ4F_VERSION)]
           (when (== 1 (long (LZ4F_isError ctx-err)))
             (throw (Exception. (str (LZ4F_getErrorName ctx-err)))))
           (Pointer. (ptrptr 0))))
        addr (.address retval)]
    (resource/track retval {:track-type :auto
                            :dispose-fn #(LZ4F_freeDecompressionContext (Pointer. addr))})))


(defn free-decomp-ctx
  [ctx]
  (LZ4F_freeDecompressionContext ctx))
