(ns eth.mjd.csv-clj-test
  (:require [clojure.test :refer :all]
            [eth.mjd.csv-clj :as csv]
            [clojure.instant :as instant])
  (:import (java.time Instant)
           (java.time.format DateTimeFormatter)))

(def unstructred-csv
  (str "2021-11-14,plants,3,\"one fewer\"\n"
       "2021-11-13,cats,2,\"active, meowing, \neating houseplants\"\n"
       "2021-11-13,plants,4"))

(def structured-csv
  (str "timestamp,subject,count,description\n"
       "2021-11-14T00:00:00Z,plants,3,one fewer\n"
       "2021-11-13T00:00:00Z,cats,2,\"active, meowing,\neating houseplants\"\n"
       "2021-11-13T00:00:00Z,plants,4,\n"))

(defn pr-iso-8601
  "Print an inst as an ISO-8601 string."
  [time-inst]
  (let [inst (Instant/ofEpochMilli (inst-ms time-inst))]
    (.format DateTimeFormatter/ISO_INSTANT inst)))

;; Nb that `count` is meant to test names conflicting with a record field name.
;; I.e. (.count record) returns something different than (:count record)
(defrecord StructuredRow [timestamp subject count description])
(def codec
  (csv/codec
    [:timestamp pr-iso-8601 instant/read-instant-date]
    [:subject name keyword]
    [:count str #(Integer/parseInt %)]
    [:description identity identity]))
(def rcodec
  (csv/record-codec
    StructuredRow
    [:timestamp pr-iso-8601 instant/read-instant-date]
    [:subject name keyword]
    [:count str #(Integer/parseInt %)]
    [:description identity identity]))

(deftest reading
  (testing "reading unstructured data"
    (let [csv (csv/read-csv unstructred-csv)]
      (is (= (count csv) 3))
      (is (= (second csv) ["2021-11-13" "cats" "2" "active, meowing, \neating houseplants"]))))

  (testing "reading structured data"
    (let [csv (csv/read-csv-with structured-csv codec)]
      (is (= (count csv) 3))
      (is (= (second csv)
             {:timestamp #inst"2021-11-13T00:00:00.000-00:00",
              :subject :cats,
              :count 2,
              :description "active, meowing,\neating houseplants"})))

    (testing "into a record"
      (let [csv (csv/read-csv-with structured-csv rcodec)]
        (is (= (count csv) 3))
        (is (= (second csv)
               (->StructuredRow
               #inst"2021-11-13T00:00:00.000-00:00",
                :cats
                2
                "active, meowing,\neating houseplants")))))))

(deftest writing
  (testing "writing unstructured data"
    (let [row-vectors (csv/read-csv unstructred-csv)]
      (is (= (-> row-vectors csv/write-csv csv/read-csv) row-vectors))))
  (testing "writing structured data"
    (let [row-maps (csv/read-csv-with structured-csv codec)
          written (csv/write-csv-with codec row-maps)]
      (is (= (csv/read-csv-with written codec) row-maps)))
    (testing "from records"
      (let [row-records (csv/read-csv-with structured-csv rcodec)
            written (csv/write-csv-with rcodec row-records)]
        (is (= (csv/read-csv-with written rcodec) row-records))))))
