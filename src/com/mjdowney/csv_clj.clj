;; TODO: Unit tests
(ns com.mjdowney.csv-clj
  "Fast CSV reading and writing for both structured and unstructured data."
  (:require [clojure.java.io :as io])
  (:refer-clojure :exclude [read write])
  (:import (de.siegmar.fastcsv.writer CsvWriter LineDelimiter)
           (java.io Closeable)
           (de.siegmar.fastcsv.reader CsvReader CloseableIterator CsvRow)
           (java.lang.reflect Method)))

(set! *warn-on-reflection* true)


;;; Base CSV reading / writing functionality

(defprotocol ICSVWriter (write [this cells] "Write a row of [string]."))
(defprotocol ICSVReader (read [this] "Read a row of [string]."))

(deftype CSVWriter [^CsvWriter delegate-writer]
  ICSVWriter (write [this cells] (.writeRow delegate-writer ^Iterable cells))
  Closeable (close [this] (.close delegate-writer)))

(deftype CSVReader [^CloseableIterator rows]
  ICSVReader (read [this] (when (.hasNext rows) (.getFields ^CsvRow (.next rows))))
  Closeable (close [this] (.close rows)))

(defn ^CSVWriter writer
  "Return a CSV writer for the given file, path, or backing writer.

  Should be used inside `with-open` to ensure that the writer is properly
  closed.

  Valid options are
    :separator (Default \\,)
    :quote (Default \\\")
    :newline (:lf (default) or :cr+lf)"
  ([x] (writer x {}))
  ([x {:keys [separator quote newline]}]
   (CSVWriter.
     (.build
       (cond-> (CsvWriter/builder)
               separator (.fieldSeparator separator)
               quote (.quoteCharacter quote)
               newline (.lineDelimiter (case newline
                                         :lf LineDelimiter/LF
                                         :cr+lf LineDelimiter/CRLF)))
       (io/writer x)))))

(defn ^CSVReader reader
  "Return a CSV reader for the given file, path, or backing reader.

  Should be used inside `with-open` to ensure that the reader is properly
  closed.

  Valid options are
    :separator (Default \\,)
    :quote (Default \\\")"
  ([x] (reader x {}))
  ([x {:keys [separator quote]}]
   (CSVReader.
     (-> (cond-> (CsvReader/builder)
                 separator (.fieldSeparator separator)
                 quote (.quoteCharacter quote))
         (.build (io/reader x))
         (.iterator)))))

(defn write-csv
  "Write rows (shaped [[string]]) with the given writer (eager)."
  [csv-writer rows]
  (run! (fn [row] (write csv-writer row)) rows))

(defn read-csv
  "Read rows (shaped [[string]]) with the given reader (lazy)."
  [csv-reader]
  (take-while some? (repeatedly (fn [] (read csv-reader)))))

(comment
  ; Writing
  (with-open [w (writer "out.csv")]
    (write-csv w [["some" "headers" "to" "write"]
                  ["foo" "bar" "baz" "qux"]]))

  ; Reading
  (with-open [r (reader "out.csv")]
    (doall (read-csv r))))


;;; Codecs for working with structured data

(defprotocol TableCodec
  (headers [this] "The headers for a the table (ordered).")
  (>row [this x] "Transform to a [string] row.")
  (<row [this x] "Transform from a [string] row."))

(defmacro codec
  "Compile a codec for reading and writing tabular CSV data.

  Takes a series of `[field-name, out-fn, in-fn]` specifications and builds
  an encoding function which transform a data point to a vector of
  [(out-fn (get data field)) ...], with each vector item corresponding to one
  of the fields, and does the inverse for the decoding function.

  E.g. for a table with elements represented by maps shaped

      {:timestamp 1635812096876
       :price 100.99M
       :market :eth-usd}

  You might compile a codec:

      (codec
        [:timestamp str #(Long/parseLong %)]
        [:price #(.toPlainString ^BigDecimal %) bigdec]
        [:market name keyword])"
  [& fields+outf+inf]
  (let [sym (gensym)]
    `(reify TableCodec
       (headers [_#]
         [~@(mapv
              (comp #(if (keyword? %) (name %) (str %)) first)
              fields+outf+inf)])
       (>row [_# ~sym]
         [~@(map
              (fn [[field outf]]
                (if (= outf `identity)
                  (list `get sym field)
                  (list outf (list `get sym field))))
              fields+outf+inf)])
       (<row [_# ~sym]
         (array-map
           ~@(mapcat
               (fn [[idx [field _ inf]]]
                 [field
                  (if (= inf `identity)
                    (list `nth sym idx)
                    (list inf (list `nth sym idx)))])
               (map-indexed vector fields+outf+inf)))))))


;; E.g. a record field named "size" can't be accessed via (.size record),
;; because "size" already refers to a method.
(defrecord Dummy [])
(defonce forbidden-names (into #{} (map #(.getName ^Method %) (.getMethods Dummy))))


(defmacro record-codec
  "Like `codec`, but takes a record type as the first argument.

  When reading a row, the parsed contents at passed to the record constructor
  *in the order that the fields are passed into this function* to construct the
  codec.

  E.g. you might

      (defrecord MyRow [timestamp price market])
      (record-codec
        MyRow
        [:timestamp str #(Long/parseLong %)]
        [:price #(.toPlainString ^BigDecimal %) bigdec]
        [:market name keyword])"
  [record-type & fields+outf+inf]
  (let [sym (gensym)]
    `(reify TableCodec
       (headers [_#]
         [~@(mapv
              (comp #(if (keyword? %) (name %) (str %)) first)
              fields+outf+inf)])
       (>row [_# ~sym]
         [~@(map
              (fn [[field outf]]
                (assert (keyword? field) "all record fields are keywords")
                ; Some fields can't be accessed via .field
                (let [get-field (if (contains? forbidden-names (name field))
                                  (list field sym)
                                  (list `..
                                        (with-meta sym {:tag record-type})
                                        (symbol (name field))))]
                  (if (= outf `identity)
                    get-field
                    (list outf get-field))))
              fields+outf+inf)])
       (<row [_# ~sym]
         (new ~record-type
           ~@(map
               (fn [[idx [_ _ inf]]]
                 (if (= inf `identity)
                   (list `nth sym idx)
                   (list inf (list `nth sym idx)))) ;; TODO: Is nth slow?
               (map-indexed vector fields+outf+inf)))))))

(comment
  ;; E.g.
  (defrecord Foo [x size])
  (macroexpand-1 `(record-codec Foo [:x identity identity] [:size str bigdec])))

(defn write-csv-with
  "Write a CSV from `data`, a series of maps / records, and a `codec`."
  [writer codec data]
  (write writer (headers codec))
  (run! (fn [row] (write writer (>row codec row))) data))

(defn read-csv-with
  "Read a CSV into a series of maps or records according to `codec`."
  [reader codec]
  ; discard headers -- codec already knows what they should be
  (let [hd (read reader)]
    (assert
      (= hd (headers codec))
      (format "headers (%s) match codec (%s)" (vec hd) (headers codec))))

  (sequence
    (comp
      (take-while some?)
      (map #(<row codec %)))
    (repeatedly (fn [] (read reader)))))


;;; Examples

(comment
  ;; The basic read / write interface is nearly a drop-in replacement for
  ;; `data.csv` (only the writer construction changes):
  (require '[com.mjdowney.csv-clj :as csv]
           '[clojure.java.io :as io])

  (with-open [writer (csv/writer (io/writer "out-file.csv"))]
    (csv/write-csv writer
                   [["abc" "def"]
                    ["ghi" "jkl"]]))

  (with-open [reader (csv/reader (io/reader "out-file.csv"))]
    (doall (csv/read-csv reader))))

(comment
  ;; The interface for typed data helps with two-way conversion of map or record
  ;; fields, and is also very fast. E.g. for some data where the :timestamp of
  ;; each row is written as an ISO-8601 string, and parsed back in as if via the
  ;; #inst reader macro.
  (def data
    [{:timestamp   #inst"2021-11-01T19:23:15"
      :double      0.123456789101112
      :description "well, that's a precise number"}
     {:timestamp   #inst"2021-11-01T19:23:14"
      :double      0.123456789101111
      :description "this row is only slightly different"}])

  ; Define the conversion on the way out
  (import '(java.time.format DateTimeFormatter) (java.time Instant))
  (defn pr-iso-8601 [time-inst]
    (let [inst (Instant/ofEpochMilli (inst-ms time-inst))]
      (.format DateTimeFormatter/ISO_INSTANT inst)))

  ; Define a codec for the out and in mapping fns for each CSV row
  (def codec
    (csv/codec
      [:timestamp pr-iso-8601 clojure.instant/read-instant-date]
      [:double str #(Double/parseDouble %)]
      [:description str str]))

  (with-open [writer (csv/writer (io/writer "out-file.csv"))]
    (csv/write-csv-with writer codec data))

  (with-open [reader (csv/reader (io/reader "out-file.csv"))]
    (first (csv/read-csv-with reader codec)))
  ;=> {:timestamp #inst"2021-11-01T19:23:15.000-00:00", :double 0.123456789101112, :description "well, that's a precise number"}
  )

(comment
  ;; You can also use the codec directly to convert to and from rows
  (csv/>row codec (first data))
  ;=> ["2021-11-01T19:23:15Z" "0.123456789101112" "well, that's a precise number"]

  (csv/<row codec ["2021-11-01T19:23:15Z" "0.123456789101112" "well, that's a precise number"])
  ;=> {:timestamp #inst"2021-11-01T19:23:15.000-00:00", :double 0.123456789101112, :description "well, that's a precise number"}

  ;; Finally, for performance, you can use records instead of maps:
  (defrecord CSVRow [timestamp double description])

  (with-open [reader (csv/reader (io/reader "out-file.csv"))]
    (first
      (csv/read-csv-with
        reader
        (csv/record-codec
          CSVRow
          [:timestamp pr-iso-8601 clojure.instant/read-instant-date]
          [:double str #(Double/parseDouble %)]
          [:description str str]))))
  ;=> #CSVRow{:timestamp #inst"2021-11-01T19:23:15.000-00:00", :double 0.123456789101112, :description "well, that's a precise number"}
  )
