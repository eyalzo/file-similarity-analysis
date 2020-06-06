# file-similarity-analysis

Compression and deduplication analysis tools, to run on local machine as jars.

## DedupEstimate

Deduplication estimator - evaluates how efficient a deduplication of a directory would be. Allows choosing from a wide-range of deduplication granularity by number of bits in the rolling-hash mask. For more details about the concept see PACK project and the related publication. 

### Build

```bash
cd /tmp
git clone https://github.com/eyalzo/file-similarity-analysis.git
cd file-similarity-analysis
mvn package
```

### Running

Usage: `DedupEstimate.jar <dir-name> <chunk-bits>`

Note: The file is loaded to memory first, and then it is being processed as a whole.
Therefore, very large files may require using the -Xmx flag to enlarge maximal memory allocation by the JVM. For example: `java -Xmx4000m -jar`

To build yourself a folder with similar files, you can run the following:

```bash
# If need to cleanup: rm -r /tmp/cnn
mkdir -p /tmp/cnn
cd /tmp/cnn
for i in {1..5}; do curl -s "https://edition.cnn.com/" -o cnn$i.html; sleep 10; done
```

### Example 1

```bash
# java -jar target/DedupEstimate.jar /tmp/cnn/ 6
Folder: /tmp/cnn
Mask bits: 6
Chunk size range: 16 - 256
Expected average chunk size: 80 (considering the min chunk size but not the max)
Allowed file size in folder: 1,000 - 4,000,000,000

Legend
------
serial - order in which files were processed
avg_chunk - average chunk size in practice
chunks - number of chunks (see mask bits above)
self_bytes - number of bytes in chunks that are identical to previous chunks in the same file
glob_bytes - number of bytes in chunks that are identical to chunks in previous files (does not include self_bytes)
dedup_ratio - overall redundancy: (self_bytes + glob_bytes) / file_size

serial     file_size bits avg_chunk    chunks    self_bytes    glob_bytes dedup_ratio file_name
1          1,130,034    6        79    14,200       175,097             0     15.495% cnn1.html
2          1,130,034    6        79    14,200       175,097       954,884     99.995% cnn2.html
3          1,130,034    6        79    14,200       175,097       954,884     99.995% cnn3.html
4          1,130,034    6        79    14,200       175,097       954,884     99.995% cnn4.html
5          1,130,034    6        79    14,200       175,097       954,884     99.995% cnn5.html
total      5,650,170    6        79    71,000       875,485     3,819,536     83.095% -
```