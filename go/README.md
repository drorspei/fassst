# CLI

## v0.4.0

* List - from any source fs
  * Options:
    * `-o out.txt` - output to file instead of StdOut
* Copy - from source to target
* ZipCopy - from source to archives in target
  * Options:
  * `--max-batch-count` - maximum number of files per archive
  * `--max-batch-size` - maximum size of files per archive
  * `--batch-across-pages` - collect results across pages to fill batches
  * `--archiving-goroutines` - limit concurrency
* Sync
  * *Not Implemented*
* Global Options
  * `--listing-goroutines` - limit concurrency
  * `--verbose` - print debug info

# API

## v0.4.0

### FS (`pkg/fs`)

* Mock FS - for testing rate limiting and pagination
  * Testing Level: Sanity (in `list_test` and `copy_test`)
* Memory FS - for testing data writes
  * Testing Level: Sanity (in `copy_test`)
* Local FS - for interacting with os fs
  * Testing Level: Untested
  * Platforms: Win64
* S3 FS - for interacting with s3
  * Testing Level: Untested
  * Missing:
    * [ ] WriteFile
    * [ ] MkDir

### Core (`pkg/fassst`)

* **List** - get all files from any fs directory recursively
  * Testing Level: Sanity
* **Copy** - copy files between file-systems and urls
  * Testing Level: Sanity
* **ZipCopy** - like **Copy** but files are archived before being saved at destination
  * Testing Level: Untested
  * Missing:
    * [ ] Logs
    * [ ] Handle Panics
* **Sync** - copy but only new or updated files.
  * *Not Implemented*