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
import java.util.List;

import com.eyalzo.common.chunks.ChunksFiles;
import com.eyalzo.common.chunks.PackChunking;
import com.eyalzo.common.files.FileUtils;

/**
 * @author Eyal Zohar
 */
public class SimAnalysis
{
	private static final long	MIN_FILE_SIZE	= 1000;
	private static final long	MAX_FILE_SIZE	= 4000000000L;
	private static final int	FILE_BLOCK_SIZE	= 1000000;

	private static void printUsage()
	{
		System.out.println("Usage: <dir-name> <chunk-bits (" + PackChunking.MIN_MASK_BITS + "-"
				+ PackChunking.MAX_MASK_BITS + ")> [print-overlap-chunks (1+)]");
		System.out.println("   chunk-bits: can be a range like \"8-9\".");
		System.out
				.println("   print-overlap-chunks: optional. Will print a list of overlap chunks according to the specified count.");
	}

	/**
	 * @param args
	 *            Command line arguments.
	 */
	public static void main(String[] args)
	{
		//
		// Get dir name from command line
		//
		if (args.length < 2)
		{
			printUsage();
			System.exit(-1);
		}

		String dirName = args[0];

		// Mask bits - can be a range
		String[] split = args[1].split("-");
		int maskBits = Integer.parseInt(split[0]);
		int maskBitsMax = split.length == 1 ? maskBits : Integer.parseInt(split[1]);
		if (maskBits < PackChunking.MIN_MASK_BITS || maskBitsMax > PackChunking.MAX_MASK_BITS || maskBitsMax < maskBits)
		{
			printUsage();
			System.exit(-1);
		}

		// Print overlap chunks?
		int printOverlapChunks = 0;
		if (args.length >= 3)
		{
			try
			{
				printOverlapChunks = Integer.parseInt(args[2]);
			} catch (NumberFormatException e)
			{
				printUsage();
				System.exit(-1);
			}
		}

		System.out.println("Directory: " + dirName);

		//
		// List files in dir
		//
		List<String> fileList = FileUtils.getDirFileListSorted(dirName, MIN_FILE_SIZE, MAX_FILE_SIZE);
		if (fileList == null || fileList.isEmpty())
		{
			System.out.println("No files in dir \"" + dirName + "\" (after filtering min-max)");
			System.exit(-2);
		}

		// Initialize the pack chunking for specific number of bits
		PackChunking pack = new PackChunking(maskBits);

		// Details relevant only to single mask-bits
		System.out.println(String.format("Mask bits: %,d", maskBits));
		System.out
				.println(String.format("Average chunk size: %,d (not considering the max)", pack.getAverageChunkLen()));
		System.out
				.println(String.format("Chunk size range: %,d - %,d", pack.getMinChunkSize(), pack.getMaxChunkSize()));
		System.out.println(String.format("Processed file size range: %,d - %,d", MIN_FILE_SIZE, MAX_FILE_SIZE));

		// Legend
		System.out.println("\nLegend\n------");
		System.out.println("name - original file name (no path)");
		System.out.println("size - file size (bytes)");
		System.out.println("chunks - number of chunks (see mask bits above)");
		System.out.println("new_chunks - number of unique chunks not found in any file before (count by unique hash)");
		System.out
				.println("overlap_bytes - overlapping bytes with previous files (does not consider identical chunks within the current file)");
		System.out
				.println("overlap_ratio - redundancy ratio when comparing with all previous files (see overlap_prev_bytes)");

		// Header
		System.out
				.println("\nserial        size bits avg_chunk    chunks   new_chunks overlap_bytes overlap_ratio name");

		//
		// Process all files
		//
		ByteBuffer buffer = ByteBuffer.allocate(FILE_BLOCK_SIZE);
		for (; maskBits <= maskBitsMax; maskBits++)
		{
			int serial = 0;
			// Initialize the pack chunking for specific number of bits
			pack = new PackChunking(maskBits);
			// Global chunk list
			ChunksFiles chunksFiles = new ChunksFiles();

			for (String curFileName : fileList)
			{
				File file = new File(curFileName);

				// Get all the file's chunks
				List<Long> curChunkList = pack.getFileChunks(curFileName, buffer);

				// Count overlapping bytes
				long overlappingBytes = chunksFiles.getOverlapsSize(curChunkList);
				double overlapRatio = file.length() <= 0 ? 0 : (overlappingBytes * 100.0) / file.length();

				// Add to the global list
				int newChunks = chunksFiles.addFile(file, curChunkList);

				// Name to print
				String nameToPrint = (new File(curFileName)).getName();

				// Print
				serial++;
				System.out.println(String.format("%-6d %,11d %4d   %,7d %,9d    %,9d   %,11d         %3.2f%% %s",
						serial, file.length(), maskBits, file.length() / curChunkList.size(), curChunkList.size(),
						newChunks, overlappingBytes, overlapRatio, nameToPrint));

				// Print overlaps, if any
				if (printOverlapChunks > 0 && overlappingBytes > 0)
					chunksFiles.printOverlaps(curChunkList, printOverlapChunks);
			}
		}
	}
}
