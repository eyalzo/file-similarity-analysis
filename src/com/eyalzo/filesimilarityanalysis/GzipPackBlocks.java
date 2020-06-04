/**
 * Copyright 2014 Eyal Zohar. All rights reserved.
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import com.eyalzo.common.chunks.PackChunking;
import com.eyalzo.common.files.FileUtils;

/**
 * @author Eyal Zohar
 * 
 */
public class GzipPackBlocks
{
	private static final long	MIN_FILE_SIZE	= 1000;
	private static final long	MAX_FILE_SIZE	= 4000000000L;

	public static int compressPackBlocks(ByteBuffer uncompressedBuffer, int maskBits, boolean debug, OutputStream os,
			Logger log) throws IOException
	{
		//
		// Get the file's chunk list
		//
		PackChunking pack = new PackChunking(maskBits);
		LinkedList<Long> chunkList = new LinkedList<Long>();
		pack.getChunks(chunkList, uncompressedBuffer.array(), uncompressedBuffer.arrayOffset(),
				uncompressedBuffer.arrayOffset() + uncompressedBuffer.limit(), true);

		GZIPOutputStream gz = new GZIPOutputStream(os, true);
		uncompressedBuffer.position(0);
		for (long curChunk : chunkList)
		{
			int curChunkLen = PackChunking.chunkToLen(curChunk);
			gz.write(uncompressedBuffer.array(), uncompressedBuffer.arrayOffset() + uncompressedBuffer.position(),
					curChunkLen);
			// This is the key trick - block is terminated and a new block will
			// be created
			gz.flush();
			// Next offset
			uncompressedBuffer.position(uncompressedBuffer.position() + curChunkLen);
		}

		gz.close();

		return chunkList.size();
	}

	private static void printUsage()
	{
		System.out.println("Compress a given file or directory with gzip while restarting every block. "
				+ "The compression uses deflate algorithm. " + "Output is written to output file(s). "
				+ "Does not process gz/zip/rar files.");
		System.out.println("Usage: <filename/dir> <chunk-bits (" + PackChunking.MIN_MASK_BITS + "-"
				+ PackChunking.MAX_MASK_BITS + ")>");
		System.out
				.println("   <filename/dir> - Existing input file name or a directory to process, full path or relative.");
		System.out.println("   <chunk-bits> - can be a range like \"8-9\".");
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException
	{
		if (args.length < 2)
		{
			printUsage();
			System.exit(-1);
		}

		// Parameter 1 - Input file name
		String fileOrDir = args[0];
		List<String> fileList = null;
		if (FileUtils.fileExists(fileOrDir))
		{
			// Single file case
			fileList = new ArrayList<String>();
			fileList.add(fileOrDir);
		} else
		{
			fileList = FileUtils.getDirFileListSorted(fileOrDir, MIN_FILE_SIZE, MAX_FILE_SIZE);
			if (fileList == null || fileList.isEmpty())
			{
				System.out.println("Could not find the file or directory '" + fileOrDir + "'");
				System.exit(-2);
			}
		}

		// Parameter 2 - PACK mask bits
		// Mask bits - can be a range
		String[] split = args[1].split("-");
		int maskBitsMin = Integer.parseInt(split[0]);
		int maskBitsMax = split.length == 1 ? maskBitsMin : Integer.parseInt(split[1]);
		if (maskBitsMin < PackChunking.MIN_MASK_BITS || maskBitsMax > PackChunking.MAX_MASK_BITS
				|| maskBitsMax < maskBitsMin)
		{
			printUsage();
			System.exit(-1);
		}

		// Header
		System.out.println("\nsize-in        size-out bits     chunks avg_chunk avg_compr  ratio name");

		for (String curFileName : fileList)
		{
			// Skip compressed files, and especially the outputs of this program
			if (curFileName.endsWith(".gz") || curFileName.endsWith(".zip") || curFileName.endsWith(".rar"))
				continue;

			// Read the entire file
			ByteBuffer buffer = FileUtils.readBlock(curFileName, 0, 0, ByteOrder.BIG_ENDIAN, null);
			if (buffer == null || buffer.remaining() <= 0)
			{
				System.err.println("Failed to read '" + curFileName + "'");
				continue;
			}

			long inputFileSize = buffer.limit();
			// System.out.println("Input file: '" + curFileName + "'");
			// System.out.println(String.format("   Size: %,d", inputFileSize));

			for (int maskBits = maskBitsMin; maskBits <= maskBitsMax; maskBits++)
			{
				String outFilename = curFileName + ".pack-" + maskBits + "bits.gz";
				FileOutputStream fileOs = new FileOutputStream(new File(outFilename));

				int chunks = compressPackBlocks(buffer, maskBits, true, fileOs, null);
				long outFileSize = FileUtils.getFileSize(outFilename, null);
				int avgChunkSize = (int) (inputFileSize / chunks);
				int avgChunkCompSize = (int) (outFileSize / chunks);

				System.out.println(String.format("%-,11d %,11d %4d %,10d %,9d %,9d %3.2f%% %s", inputFileSize,
						outFileSize, maskBits, chunks, avgChunkSize, avgChunkCompSize, outFileSize * 100.0
								/ inputFileSize, outFilename));
			}
		}
	}
}
