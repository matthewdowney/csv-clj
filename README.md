# csv-clj

Write CSV files 10x faster than clojure/data.csv and work 
easily with structured data. Built on top of the excellent 
[osiegmar/FastCSV](https://github.com/osiegmar/FastCSV) Java library.

v0.0.2 | [CHANGELOG](CHANGELOG.md) | Uses [Break Versioning](https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md)

## Usage

The basic read / write interface is nearly a drop-in replacement for
`data.csv` (only the writer construction changes):
```clojure
;; The basic read / write interface is nearly a drop-in replacement for
;; `data.csv` (only the writer construction changes):
(require '[eth.mjd.csv-clj :as csv]
         '[clojure.java.io :as io])

(with-open [writer (csv/writer (io/writer "out-file.csv"))]
  (csv/write-csv writer
                 [["abc" "def"]
                  ["ghi" "jkl"]]))

(with-open [reader (csv/reader (io/reader "out-file.csv"))]
  (doall (csv/read-csv reader)))
;=> (["abc" "def"] ["ghi" "jkl"])
```

## Benchmarks

Benchmarks for writing and then reading 1 million rows:

|                    | csv-clj | clojure.data/csv | Faster by |
|--------------------|---------|------------------|-----------|
| Write unstructured |   236ms |           3004ms |    12.73x |
|   Write structured |   210ms |           2940ms |    14.00x |
|  Read unstructured |   324ms |           1422ms |     4.39x |
|    Read structured |   553ms |           1873ms |     3.39x |


Run with `clj -X:bench`.

## Structured data

The interface for structured data helps with two-way conversion of map or 
record fields, and is also very fast. E.g. for some data where the :timestamp 
of each row is written as an ISO-8601 string, and parsed back in as if via the
`#inst` reader macro.

```clojure
(def data
  [{:timestamp #inst"2021-11-01T19:23:15"
    :double 0.123456789101112
    :description "well, that's a precise number"}
   {:timestamp #inst"2021-11-01T19:23:14"
    :double 0.123456789101111
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
  (csv/write-csv-with codec writer data))

(with-open [reader (csv/reader (io/reader "out-file.csv"))]
  (first (csv/read-csv-with reader codec)))
;=> {:timestamp #inst"2021-11-01T19:23:15.000-00:00", :double 0.123456789101112, :description "well, that's a precise number"}
```

You can also use the codec directly to convert to and from rows:

```clojure
(csv/>row codec (first data))
;=> ["2021-11-01T19:23:15Z" "0.123456789101112" "well, that's a precise number"]

(csv/<row codec ["2021-11-01T19:23:15Z" "0.123456789101112" "well, that's a precise number"])
;=> {:timestamp #inst"2021-11-01T19:23:15.000-00:00", :double 0.123456789101112, :description "well, that's a precise number"}
```

Finally, for peak performance, you can represent rows with records instead of 
maps:
```clojure
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
```
