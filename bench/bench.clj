(ns bench
  "Read and write some data, both with this library and with clojure.data/csv,
  and print the results in a table.

  Invoke with `clj -X:bench`, or `clj -X:bench :n 100000` to restrict the
  number of rows used."
  (:require [clojure.data.csv :as data.csv]
            [com.mjdowney.csv-clj :as csv]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint])
  (:import (java.util Random)))

(set! *warn-on-reflection* true)


;;; Data used for benchmarking
(def max-rows 5000000)
(def alphabet (vec (concat [\" \,] (map char (range 58 97)))))

(def unstructured-data
  (let [r (Random. 1635792327272)]
    (for [_ (range max-rows)]
      (let [row-len (inc (.nextInt r 8))]
        (vec
          (for [_ (range row-len)]
            (let [cell-len (.nextInt r 8)]
              (apply str
                     (for [_ (range cell-len)]
                       (nth alphabet (.nextInt r (count alphabet))))))))))))

(defrecord BenchmarkRow [foo bar baz qux])
(def structured-data
  (let [r (Random. 1635792327272)]
    (for [_ (range max-rows)]
      (let [cell #(let [cell-len (.nextInt r 8)]
                    (apply str
                           (for [_ (range cell-len)]
                             (nth alphabet (.nextInt r (count alphabet))))))]
        (->BenchmarkRow (cell) (cell) (cell) (cell))))))


;;; Simple benchmark

(defn clojure-data-csv-unstructured-write [data]
  (with-open [w (io/writer "bench/data.csv")]
    (data.csv/write-csv w data)))

(defn clojure-data-csv-unstructured-read []
  (with-open [r (io/reader "bench/data.csv")]
    (doall (data.csv/read-csv r))))

(defn unstructured-write [data]
  (with-open [w (csv/writer "bench/csv-clj")]
    (csv/write-csv w data)))

(defn unstructured-read []
  (with-open [r (csv/reader "bench/csv-clj")]
    (doall (csv/read-csv r))))


;;; Benchmark with structured data

(defn clojure-data-csv-structured-write [data]
  (with-open [w (io/writer "bench/data.csv")]
    (let [x (first data)]
      (data.csv/write-csv
        w
        (cons
          (map name (keys x)) ; headers
          (map vals data)))))) ; rows

(defn clojure-data-csv-structured-read []
  (with-open [r (io/reader "bench/data.csv")]
    (let [data (data.csv/read-csv r)
          headers (map keyword (first data))]
      (->> (rest data)
           (map #(zipmap headers %))
           doall))))

(def codec
  (csv/record-codec
    BenchmarkRow
    [:foo identity identity]
    [:bar identity identity]
    [:baz identity identity]
    [:qux identity identity]))

(defn structured-write [data]
  (with-open [w (csv/writer "bench/csv-clj")]
    (csv/write-csv-with w codec data)))

(defn structured-read []
  (with-open [r (csv/reader "bench/csv-clj")]
    (doall (csv/read-csv-with r codec))))


;;; Code to run the benchmarks

(defn median [nums]
  (loop [nums (vec (sort nums))]
    (let [cnt (count nums)]
      (cond
        (= cnt 1) (first nums)
        (= cnt 2) (/ (apply + nums) 2.0)
        :else (recur (subvec nums 1 (dec cnt)))))))

(defn bench* [dbg f trials]
  (let [times (map
                (fn [trial]
                  (print (format "#%-2s Timing %s... " trial dbg))
                  (flush)
                  (let [start (System/currentTimeMillis)]
                    (f)
                    (let [elapsed (- (System/currentTimeMillis) start)]
                      (print elapsed)
                      (println "msecs")
                      elapsed)))
                (range trials))
        m (median times)]
    (println (format "Median time for %s is %smsecs\n" dbg m))
    m))

(defmacro bench [f trials & [?data]]
  (if ?data
    `(let [data# ~?data]
       (bench* ~(str f) (fn [] (~f data#)) ~trials))
    `(bench* ~(str f) ~f ~trials)))

(defn write-benchmarks [n]
  (io/make-parents "bench/test.csv")
  (let [_ (println "Generating benchmark data...")
        udata (into [] (take n) unstructured-data)
        sdata (into [] (take n) structured-data)

        tbl [{""                 (str "Write to disk")
              "csv-clj"          (str (bench unstructured-write 3 udata) "ms")
              "clojure.data/csv" (str (bench clojure-data-csv-unstructured-write 3 udata) "ms")}
             {""                 (str "Write structured data to disk")
              "csv-clj"          (str (bench structured-write 3 sdata) "ms")
              "clojure.data/csv" (str (bench clojure-data-csv-structured-write 3 sdata) "ms")}]]
    tbl))

(defn read-benchmarks []
  (let [tbl [{""                 (str "Read from disk")
              "csv-clj"          (str (bench unstructured-read 3) "ms")
              "clojure.data/csv" (str (bench clojure-data-csv-unstructured-read 3) "ms")}
             {""                 (str "Read structured data from disk")
              "csv-clj"          (str (bench structured-read 3) "ms")
              "clojure.data/csv" (str (bench clojure-data-csv-structured-read 3) "ms")}]]
    tbl))

(defn benchmark [& {:keys [n] :or {n max-rows}}]
  (assert (<= n max-rows))
  (let [nstr (apply
               format
               (cond
                 (> n 1000000) ["%smm" (/ n 1000000.0)]
                 (> n 1000) ["%sk" (/ n 1000.0)]
                 :else ["%s" n]))
        _ (println "Benchmarking with" nstr "rows of data...")
        wbs (write-benchmarks n)
        rbs (read-benchmarks)]
    (println "Benchmarks for writing and then reading" nstr "rows:")
    (pprint/print-table (concat wbs rbs))))

(comment
  ;; Run like so
  (benchmark)
  (benchmark :n 100000))
