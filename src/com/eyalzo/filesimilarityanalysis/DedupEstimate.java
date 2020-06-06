/**
 * Copyright 2011 Eyal Zohar. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY EYAL ZOHAR ''AS IS'' AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL EYAL ZOHAR OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are
 * those of the authors and should not be interpreted as representing official
 * policies, either expressed or implied, of Eyal Zohar.
 */
package com.eyalzo.filesimilarityanalysis;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;

import com.eyalzo.common.chunks.PackChunking;
import com.eyalzo.common.files.FileUtils;

/**
 * @author Eyal Zohar
 */
public class DedupEstimate {
	private static final long MIN_FILE_SIZE = 1000;
	private static final long MAX_FILE_SIZE = 4000000000L;
	private static final int FILE_BLOCK_SIZE = 1000000;

	private static void printUsage() {
		System.out.println("Usage: <dir-name> <chunk-bits (" + PackChunking.MIN_MASK_BITS + "-"
				+ PackChunking.MAX_MASK_BITS + ")>");
		System.out.println("   chunk-bits: can be a range like \"8-9\".");
	}

	/**
	 * @param args
	 *            Command line arguments.
	 */
	public static void main(String[] args) {
		//
		// Get dir name from command line
		//
		if (args.length < 2) {
			printUsage();
			System.exit(-1);
		}

		String dirName = args[0];

		// Mask bits - can be a range
		String[] split = args[1].split("-");
		int maskBits = Integer.parseInt(split[0]);
		int maskBitsMax = split.length == 1 ? maskBits : Integer.parseInt(split[1]);
		if (maskBits < PackChunking.MIN_MASK_BITS || maskBitsMax > PackChunking.MAX_MASK_BITS
				|| maskBitsMax < maskBits) {
			printUsage();
			System.exit(-1);
		}

		System.out.println("Folder: " + dirName);

		//
		// List files in dir
		//
		List<String> fileList = FileUtils.getDirFileListSorted(dirName, MIN_FILE_SIZE, MAX_FILE_SIZE);
		if (fileList == null || fileList.isEmpty()) {
			System.out.println("No files in folder \"" + dirName + "\" (after filtering min-max)");
			System.exit(-2);
		}

		// Initialize the pack chunking for specific number of bits
		PackChunking pack = new PackChunking(maskBits);

		// Details relevant only to single mask-bits
		System.out.println(String.format("Mask bits: %,d", maskBits));
		System.out
				.println(String.format("Chunk size range: %,d - %,d", pack.getMinChunkSize(), pack.getMaxChunkSize()));
		System.out.println(
				String.format("Expected average chunk size: %,d (considering the min chunk size but not the max)",
						pack.getAverageChunkLen()));
		System.out.println(String.format("Allowed file size in folder: %,d - %,d", MIN_FILE_SIZE, MAX_FILE_SIZE));

		// Legend
		System.out.println("\nLegend\n------");
		System.out.println("serial - order in which files were processed");
		System.out.println("avg_chunk - average chunk size in practice");
		System.out.println("chunks - number of chunks (see mask bits above)");
		System.out.println(
				"self_bytes - number of bytes in chunks that are identical to previous chunks in the same file");
		System.out.println(
				"glob_bytes - number of bytes in chunks that are identical to chunks in previous files (does not include self_bytes)");
		System.out.println("dedup_ratio - overall redundancy: (self_bytes + glob_bytes) / file_size");

		// Header
		System.out.println(
				"\nserial     file_size bits avg_chunk    chunks    self_bytes    glob_bytes dedup_ratio file_name");

		ByteBuffer buffer = ByteBuffer.allocate(FILE_BLOCK_SIZE);
		for (; maskBits <= maskBitsMax; maskBits++) {
			int serial = 0;
			// Initialize the pack chunking for specific number of bits
			pack = new PackChunking(maskBits);
			// Global chunk list
			HashSet<Long> globalChunks = new HashSet<Long>();

			//
			// Process all files
			//
			long totalSize = 0;
			long totalChunks = 0;
			long totalSelfDedup = 0;
			long totalGlobalDedup = 0;

			for (String curFileName : fileList) {
				File file = new File(curFileName);

				// Get all the file's chunks
				List<Long> curChunkList = pack.getFileChunks(curFileName, buffer);
				HashSet<Long> curChunkDedupChunks = new HashSet<Long>();

				// Count overlapping bytes
				long curSelfDedupBytes = 0;
				long curGlobalDedupBytes = 0;

				for (Long curChunk : curChunkList) {
					int curChunkLen = PackChunking.chunkToLen(curChunk);
					// If self-dedup
					if (curChunkDedupChunks.contains(curChunk)) {
						curSelfDedupBytes += curChunkLen;
						continue;
					}

					// Remember for next instance of the same chunk (self dedup)
					curChunkDedupChunks.add(curChunk);

					// If self-dedup
					if (globalChunks.contains(curChunk)) {
						curGlobalDedupBytes += curChunkLen;
					}
				}

				long curDedupBytes = curSelfDedupBytes + curGlobalDedupBytes;
				double overlapRatio = file.length() <= 0 ? 0 : (curDedupBytes * 100.0) / file.length();

				// Add to the global list
				globalChunks.addAll(curChunkList);

				// Totals
				totalSize += file.length();
				totalChunks += curChunkList.size();
				totalSelfDedup += curSelfDedupBytes;
				totalGlobalDedup += curGlobalDedupBytes;

				// Name to print
				String nameToPrint = (new File(curFileName)).getName();

				// Print
				serial++;
				System.out.println(String.format("%-6d %,13d %4d %,9d %,9d %,13d %,13d %10.3f%% %s", serial,
						file.length(), maskBits, file.length() / curChunkList.size(), curChunkList.size(),
						curSelfDedupBytes, curGlobalDedupBytes, overlapRatio, nameToPrint));
			}

			// End of files look
			System.out.println(String.format("total  %,13d %4d %,9d %,9d %,13d %,13d %10.3f%% -", totalSize, maskBits,
					totalSize / totalChunks, totalChunks, totalSelfDedup, totalGlobalDedup,
					(totalSelfDedup + totalGlobalDedup) * 100.0 / totalSize));
		}
	}
}
