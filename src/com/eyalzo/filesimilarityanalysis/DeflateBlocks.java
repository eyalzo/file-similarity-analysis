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
import java.util.zip.GZIPOutputStream;

import com.eyalzo.common.files.FileUtils;

/**
 * 
 * Works with Java 7 only, because Java 6 does not expose the flush function of
 * gzip.
 * 
 * @author Eyal Zohar
 */
public class DeflateBlocks
{
	private static void printUsage()
	{
		System.out.println("Compress a given file with gzip while restarting every block. "
				+ "The compression uses deflate algorithm. " + "Output is written in memory or to output file.");
		System.out.println("Usage: <filename> <block size>");
		System.out.println("   <filename> - Existing input file name, full path or relative.");
		System.out.println("   <block size> - The block size to test. Use zero for automatic increment (*2 step), "
				+ "or a given value for a single test.");
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException
	{
		//
		// Get filename from command line
		//
		if (args.length < 2)
		{
			printUsage();
			System.exit(-1);
		}

		String fileName = args[0];
		if (!FileUtils.fileExists(fileName))
		{
			System.out.println("Could not find the file '" + fileName + "'");
			System.exit(-2);
		}

		int paramBlockSize = Integer.parseInt(args[1]);

		// Read the entire file
		ByteBuffer buffer = FileUtils.readBlock(fileName, 0, 0, ByteOrder.BIG_ENDIAN, null);
		if (buffer == null || buffer.remaining() <= 0)
		{
			System.out.println("Failed to read '" + fileName + "'");
			System.exit(-3);
		}

		int fileSize = buffer.limit();

		int level = 6;
		System.out.println("Input file: '" + fileName + "'");
		System.out.println(String.format("   Size: %,d", fileSize));
		System.out.println();
		System.out.println("level       block      output ratio file");

		// Loop (unless block size was specified)
		int blockSize = (paramBlockSize > 0) ? paramBlockSize : 128;
		while (true)
		{
			String outFilename = fileName + ".block-" + (blockSize >= fileSize ? "atonce" : blockSize) + ".gz";

			FileOutputStream fileOs = new FileOutputStream(new File(outFilename));

			try
			{
				writeToGzipStreamJava7(buffer, fileOs, blockSize);
			} catch (IOException e)
			{
				e.printStackTrace();
				System.exit(-6);
			}

			long writtenBytes = FileUtils.getFileSize(outFilename, null);
			System.out.println(String.format("%,5d %,11d %,11d %.1f%% %s", level, blockSize, writtenBytes, writtenBytes
					* 100.0 / fileSize, outFilename));

			if (paramBlockSize > 0)
				break;

			blockSize *= 1.5;
			if (blockSize > fileSize)
				break;
		}
	}

	private static boolean writeToGzipStreamJava7(ByteBuffer uncompressedBuffer, OutputStream os, int blockSize)
			throws IOException
	{
		GZIPOutputStream gz = new GZIPOutputStream(os, true);
		for (int offset = 0; offset < uncompressedBuffer.capacity(); offset += blockSize)
		{
			gz.write(uncompressedBuffer.array(), uncompressedBuffer.arrayOffset() + offset,
					Math.min(blockSize, uncompressedBuffer.capacity() - offset));
			// This is the key trick - block is terminated and a new block will
			// be created
			gz.flush();
		}

		gz.close();

		return true;
	}
}