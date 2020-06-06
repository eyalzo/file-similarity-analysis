/**
 * Copyright 2011 Eyal Zohar. All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
 * following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
 * disclaimer.
 * 
 * 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY EYAL ZOHAR ''AS IS'' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * EYAL ZOHAR OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * 
 * The views and conclusions contained in the software and documentation are those of the authors and should not be
 * interpreted as representing official policies, either expressed or implied, of Eyal Zohar.
 */
package com.eyalzo.common.chunks;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * Global list of chunks and the files where they appear.
 * 
 * @author Eyal Zohar
 * 
 */
public class ChunksFiles {
	private class FileAndOffset {
		File file;
		long offset;

		public FileAndOffset(File file, long offset) {
			this.file = file;
			this.offset = offset;
		}
	};

	/**
	 * Key is a chunk and value is a file list that have this chunk.
	 */
	private HashMap<Long, HashSet<FileAndOffset>> chunksFiles = new HashMap<Long, HashSet<FileAndOffset>>();

	/**
	 * 
	 * @param file
	 *            File object.
	 * @param chunkList
	 *            Chunk list, maybe with duplicates.
	 * @return Number of new chunks, that were not in the list before. Duplicates within the file are considered only once.
	 */
	public int addFile(File file, List<Long> chunkList) {
		// Sanity check
		if (file == null || chunkList == null || chunkList.isEmpty())
			return 0;

		int result = 0;
		long offset = 0;

		// Walk through the chunks
		for (Long curChunk : chunkList) {
			HashSet<FileAndOffset> files = chunksFiles.get(curChunk);
			if (files == null) {
				// Create a new file list for the current chunk
				files = new HashSet<FileAndOffset>();
				chunksFiles.put(curChunk, files);
				// Indicate that the current chunk is new to this list
				result++;
			}

			// Add the file to the file list for the current chunk
			files.add(new FileAndOffset(file, offset));

			offset += PackChunking.chunkToLen(curChunk);
		}

		return result;
	}

	public void printOverlaps(List<Long> chunkList, int maxChunksToPrint) {
		if (chunkList == null)
			return;

		System.out.println("    serial  hash         size    offset1   offset2   file2");
		System.out.println("    ------- ------------ ------- --------- --------- -------------------");

		int offset = 0;
		int chunkSerial = 1;
		for (long curChunk : chunkList) {
			HashSet<FileAndOffset> files = chunksFiles.get(curChunk);
			if (files != null) {
				if (maxChunksToPrint <= 0) {
					System.out.println("   ...");
					return;
				}

				for (FileAndOffset curFileAndOffset : files) {
					System.out.println(
							String.format("    %,7d %s %,9d %,9d %s", chunkSerial, PackChunking.chunkToString(curChunk),
									offset, curFileAndOffset.offset, curFileAndOffset.file.getPath()));
				}

				maxChunksToPrint--;
			}

			offset += PackChunking.chunkToLen(curChunk);
			chunkSerial++;
		}
	}

	/**
	 * Get the number of bytes in a given chunk list that overlaps the global list.
	 * 
	 * @param chunkList
	 *            Chunk list.
	 * @return Number of bytes in a given chunk list that overlaps the global list.
	 */
	public long getOverlapsSize(List<Long> chunkList) {
		if (chunkList == null)
			return 0;

		long result = 0;

		for (long curChunk : chunkList) {
			if (chunksFiles.containsKey(curChunk))
				result += PackChunking.chunkToLen(curChunk);
		}

		return result;
	}
}
