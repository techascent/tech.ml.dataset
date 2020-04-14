# Changelog

## 2.0-beta-12
* Support for parsing and working with durations.  Strings that look like times -
   "00:00:12" will be parsed into hh:mm:ss durations.  The value can have a negative
   sign in front.  This is in addition to the duration's native serialization string
   type.
* Added short test for tensors in datasets.  This means that the venerable print-table
  is no longer enough as it doesn't account for multiline strings and thus datatets
  with really complex things will not print correctly for a time.

## 2.0-beta-11
* Various fixes related to parsing and working with open data.
* `tech.ml.dataset.column/parse-column` - given a string column that failed to parse for
  some reason, you can force the system to attempt to parse it using, for instance,
  relaxed parsing semantics where failures simply record the failure in metadata.
* relaxed parsing in general is supported across all input types.

## 0.26
### Added
* rolling (rolling windows of computed functions - math operation)
* dataset/dssort-by
* dataset/ds-take-nth

## 0.22
### Added
* PCA
