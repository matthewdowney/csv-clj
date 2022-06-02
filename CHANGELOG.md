# Changelog

## v0.0.2
- Allow reading and writing structured CSVs without headers.

## v0.0.1

- Unit tests.
- Realization that fast record field access is not a good idea when field names
  conflict with record methods (i.e. `(= (.foo x) (:foo x))` but 
  `(not= (.count x) (:count x))`) and subsequent fix.
- Initial functionality and benchmarks.
