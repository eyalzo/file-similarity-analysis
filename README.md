# file-similarity-analysis

Compression and deduplication analysis tools, designed to run on a local machine as JAR files. These tools were developed by Eyal Zohar as part of research activities published in:

1. [The Power of Prediction: Cloud Bandwidth and Cost Reduction](https://dl.acm.org/doi/10.1145/2018436.2018447)
2. [Celleration: Loss-Resilient Traffic Redundancy Elimination for Cellular Data](https://dl.acm.org/doi/pdf/10.1145/2162081.2162096)

---

## Tools Overview

### DedupEstimate

The `DedupEstimate` tool evaluates how efficient deduplication of a directory would be. It allows users to choose a wide range of deduplication granularities by specifying the number of bits in the rolling-hash mask. For more details about the concept, see the PACK project and the related publications.

---

## Build Instructions

To build the project, ensure you have [Maven](https://maven.apache.org/) installed, then run the following commands:

```bash
cd /tmp
git clone https://github.com/eyalzo/file-similarity-analysis.git
cd file-similarity-analysis
mvn package
```

This will generate the JAR files in the `target` directory.

---

## Running the Tools

### DedupEstimate

#### Usage

```bash
java -jar target/DedupEstimate.jar <dir-name> <chunk-bits>
```

- `<dir-name>`: The directory containing the files to analyze.
- `<chunk-bits>`: The number of bits for the rolling-hash mask. This can also be a range (e.g., `6-8`).

#### Notes

- The tool loads files into memory before processing them. For very large files, you may need to increase the JVM's maximum memory allocation using the `-Xmx` flag. For example:
  ```bash
  java -Xmx4000m -jar target/DedupEstimate.jar <dir-name> <chunk-bits>
  ```

---

### Example: DedupEstimate

To create a folder with similar files for testing, you can use the following commands:

```bash
# Cleanup if needed
rm -r /tmp/cnn

# Create a folder and download similar files
mkdir -p /tmp/cnn
cd /tmp/cnn
for i in {1..5}; do curl -s "https://edition.cnn.com/" -o cnn$i.html; sleep 10; done
```

Run the `DedupEstimate` tool on the folder:

```bash
java -jar target/DedupEstimate.jar /tmp/cnn/ 6
```

Sample output:

```plaintext
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

---

## License

This project is licensed under the BSD license. See the source code for details.