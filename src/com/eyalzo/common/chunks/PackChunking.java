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

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.eyalzo.common.files.FileUtils;
import com.eyalzo.common.misc.MapCounter;

/**
 * PACK chunking is a new alternative for Rabin fingerprinting traditionally used by RE applications.
 * <p>
 * Experiments show that this approach can reach data processing speeds over 3 Gbps, at least 20% faster than Rabin fingerprinting. For more details, see "The
 * Power of Prediction: Cloud Bandwidth and Cost Reduction", SIGCOMM 2011.
 * <p>
 * The idea behind PACK chunking is to hold the entire sliding window in a single 64-bit value. This value is compared with a predetermined mask that ensures
 * that each of the recent window bytes participates in the mask comparison. In addition, each of the mask's bits covers exactly 8 bytes. This is why the right
 * most 7 bits are not part of the mask, as well as the left most 7 bits.
 * <p>
 * In the illustration below the first line represents the last received byte (called "byte n") as well as its 63 ancestors. Since our sliding window needs to
 * cover 48 bytes, we do not need "byte n-48" to participate in the mask, nor any of its ancestors. This is why the presented mask (13-bit) has a binary 1 in
 * position 48. After 8 inserts bit 8 is ready, meaning it covers 8 bytes. After 48 inserts position 48 is ready too, while byte n-47 still has its last LSB
 * still in the window. So a warm-up of 47 inserts is required before the first insert-check loop.
 * 
 * <pre>
 *            64       56       48       40       32       24       16     9 8      1 
 *           |--------|--------|--------|--------|--------|--------|--------|--------|
 * byte n                                                                   |--------|
 * byte n-1                                                                -|------- |
 * byte n-2                                                               --|------  |
 * byte n-3                                                              ---|-----   |
 * byte n-4                                                             ----|----    |
 * byte n-5                                                            -----|---     |
 * byte n-6                                                           ------|--      |
 * byte n-7                                                          -------|-       |
 *                                       ....
 * byte n-46 |        |  ------|--
 * byte n-47 |        | -------|-
 * byte n-48 |        |--------
 * byte n-49 |       -|-------
 * byte n-50 |      --|------
 * byte n-51 |     ---|-----
 * byte n-52 |    ----|----
 * byte n-53 |   -----|---
 * byte n-54 |  ------|--
 * byte n-55 | -------|-
 * byte n-56 |--------|
 * byte n-57 |------- |
 * byte n-58 |------  |
 * byte n-59 |-----   |
 * byte n-60 |----    |
 * byte n-61 |---     |
 * byte n-62 |--      |
 * byte n-63 |-       |
 * 
 * mask      |00000000|00000000|10001010|00110001|00010000|01011000|00110000|10000000|
 * mask hex      00       00        8A      31       10       58       30       80
 * </pre>
 * 
 * <h2>About Chunk Size</h2>
 * 
 * Nice explanation is found in "Bimodal Content Defined Chunking for Backup Streams", by Kruus et al. The text below is taken from this paper with adaptations
 * to the terminology used in the code:
 * <p>
 * Content-defined chunking works by selecting a set of locations, called anchors, to break apart an input stream, where the chunking decision is based on the
 * contents of the data itself. This involves evaluating a bit scrambling function (say, PACK chunking) on a fixed-size 48-bytes sliding window into the data
 * stream. The result of the function is compared at some number l of bit locations with a predefined value, and if equivalent the first byte of the window is
 * considered an anchor. This generates an average chunk size of 2^l, following a geometric distribution. The probability of identifying a unique anchor is
 * maximized when the region searched is of size 2^l.
 * 
 * <h3>Minimal and Maximal Chunks</h3>
 * 
 * For minimum chunk size m, the nominal average chunk size is m + (2^l). For a maximum chunk size M, a chunker will hit the maximum with probability
 * approximately exp(−(M−m)/2^l), which can be quite frequent.
 * 
 * @author Eyal Zohar
 * 
 */
public class PackChunking {
	//
	// Mask bits
	//

	/**
	 * Mask value is determined by the given number of mask bits.
	 */
	private final long maskValue;
	/**
	 * Number of bits in a mask, which determines the chance to find an anchor in a given offset: 1/(2^{@link #maskBits} ). Equals 0 on initialization error.
	 * 
	 * @see #MIN_MASK_BITS
	 * @see #MAX_MASK_BITS
	 */
	private final int maskBits;
	/**
	 * Minimal number of bits in mask (2^8 = 256 bytes).
	 * <p>
	 * Beware to add the appropriate switch-case upon change to this value.
	 */
	public final static int MIN_MASK_BITS = 6;
	/**
	 * Maximal number of bits in mask (2^13 = 8K bytes).
	 * <p>
	 * Beware to add the appropriate switch-case upon change to this value.
	 */
	public final static int MAX_MASK_BITS = 15;

	//
	// Chunk size parameters, derived from number of mask bits
	//
	private static final int MIN_CHUNK_DIVIDER = 4;
	private static final int MAX_CHUNK_FACTOR = 4;

	/**
	 * {@link #minChunkSize} = 2^{@link #maskBits} / {@link #MIN_CHUNK_DIVIDER}
	 */
	private final int minChunkSize;

	/**
	 * {@link #maxChunkSize} = 2^{@link #maskBits} * {@link #MAX_CHUNK_FACTOR}
	 */
	private final int maxChunkSize;

	/**
	 * This is not really the average because the cut-off at the maximum also has an influence on the real average, but this is close enough if the maximum is high.
	 * 
	 * {@link #avgChunkSize} = 2^{@link #maskBits} + {@link #minChunkSize}
	 */
	private final int avgChunkSize;

	/**
	 * Number of bytes in a fingerprint.
	 */
	public final static int WINDOW_BYTES_LEN = 48;

	/**
	 * For sha-1.
	 */
	public static MessageDigest mdSha1 = null;
	/**
	 * For MD5 (faster than SHA-1).
	 */
	public static MessageDigest mdMd5 = null;

	//
	// Save chunk signature and length in a single 64 bits value
	//
	// 19 bits for length and 45 for the sha1
	/**
	 * 45 bits for the SHA-1, out of 160 bits.
	 */
	private static final long CHUNK_SHA1_MASK = 0x00001fffffffffffL;
	private static final int CHUNK_SHA1_BITS = 45;
	/**
	 * 19 bits for the length (up to
	 */
	private static final long CHUNK_LEN_MASK = 0x00007ffff;

	/**
	 * Initialize an instance of PACK chunking with specific number of bits in mask.
	 * <p>
	 * Also sets default minimum and maximum chunk size.
	 * 
	 * @param maskBits
	 *            Number of bits in a mask, which determines the chance to find an anchor in a given offset: 1/(2^ {@link #maskBits}). Equals 0 on initialization
	 *            error.
	 * 
	 * @see #MIN_MASK_BITS
	 * @see #MAX_MASK_BITS
	 */
	public PackChunking(int maskBits) {
		initMd();

		// Set mask bits
		if (maskBits < MIN_MASK_BITS)
			this.maskBits = 0;
		else if (maskBits > MAX_MASK_BITS)
			this.maskBits = 0;
		else
			this.maskBits = maskBits;

		// Calculate once, to cache the results
		minChunkSize = (int) (Math.pow(2, this.maskBits) / MIN_CHUNK_DIVIDER);
		maxChunkSize = (int) (Math.pow(2, this.maskBits) * MAX_CHUNK_FACTOR);
		avgChunkSize = (int) (Math.pow(2, this.maskBits) + minChunkSize);

		// Set the mask itself
		switch (maskBits) {
		case MIN_MASK_BITS:
			maskValue = 0x0000001010482080L;
			break;
		case 7:
			maskValue = 0x0000081010482080L;
			break;
		case 8:
			maskValue = 0x0000821010482080L;
			break;
		case 9:
			maskValue = 0x0000821110482080L;
			break;
		case 10:
			maskValue = 0x0000823110482080L;
			break;
		case 11:
			maskValue = 0x00008A3110482080L;
			break;
		case 12:
			maskValue = 0x00008A3110483080L;
			break;
		case 13:
			maskValue = 0x00008A3110583080L;
			break;
		case 14:
			maskValue = 0x00008A3110583280L;
			break;
		case MAX_MASK_BITS:
			maskValue = 0x00008A3114583280L;
			break;
		default:
			maskValue = 0;
		}
	}

	/**
	 * Initialize the SHA-1 engine.
	 */
	private static void initMd() {
		if (mdMd5 != null)
			return;

		try {
			mdSha1 = MessageDigest.getInstance("SHA1");
		} catch (NoSuchAlgorithmException e) {
			// This should never happen
			e.printStackTrace();
		}

		try {
			mdMd5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			// This should never happen
			e.printStackTrace();
		}
	}

	/**
	 * Expected number of anchors in a given buffer, without any limits on minimal or maximal chunk size. Considers the fact that the last 47 offsets are not used,
	 * because of the sliding window size (48).
	 * 
	 * @param bufferSize
	 *            The given byte buffer size.
	 * @return Number of expected anchors, or zero on input error.
	 */
	public int expectedAnchorCount(int bufferSize) {
		// Sanity check
		if (bufferSize < WINDOW_BYTES_LEN)
			return 0;

		return (int) ((bufferSize - WINDOW_BYTES_LEN + 1) / Math.pow(2, this.maskBits));
	}

	/**
	 * Expected number of chunks in a given buffer, with limit on minimal chunk size and not limit on maximal. Considers the fact that the last 47 offsets are not
	 * used, because of the sliding window size (48).
	 * 
	 * @param bufferSize
	 *            The given byte buffer size.
	 * @return Number of expected chunks, or zero on input error. Does not consider the maximal chunk size.
	 */
	public int expectedChunkCount(int bufferSize) {
		// Sanity check
		if (bufferSize < WINDOW_BYTES_LEN)
			return 0;

		return (int) ((bufferSize - WINDOW_BYTES_LEN + 1) / (minChunkSize + Math.pow(2, this.maskBits)));
	}

	/**
	 * Count number of anchors in a given buffer, without any limits on minimal or maximal chunk size.
	 * <p>
	 * When using for statistical analysis, beware that anchors cannot be found in the last 47 offsets, because of the sliding window size (48).
	 * 
	 * @param buffer
	 *            The given byte buffer.
	 * @return Number of anchors found, or zero on input error.
	 */
	public int anchorCount(byte[] buffer) {
		return anchorCount(buffer, 0, buffer.length);
	}

	/**
	 * Get chunks in a given buffer, with limits on minimal or maximal chunk size. It assumes that the start offset is the beginning of a chunk but the last bytes
	 * are not.
	 * <p>
	 * When using for statistical analysis, beware that anchors cannot be found in the last 47 offsets, because of the sliding window size (48). The last chunk is
	 * added only if it si not too small and also asked specifically.
	 * 
	 * @param buffer
	 *            The given byte buffer.
	 * @param startOffsetInc
	 *            Start offset (0-based) of the search. The first anchor can be found in this offset, but involving the 47 bytes after it.
	 * @param endOffsetExc
	 *            Exclusive 0-based end offset of the search. The last anchor can be found only 48 bytes before that offset.
	 * @param addLastChunk
	 *            Use true only if this buffer is the end of a stream/file. Otherwise, the returned offset should be used for further processing. If true the last
	 *            chunk is added too, unless it is too small.
	 * @return Number of used bytes which is the offset of the byte right after the last byte of the last found anchor. Zero on input error or when not even one
	 *         anchor was found and buffer length does not exceed the maximum chunk size.
	 */
	public int getChunks(Collection<Long> chunkList, byte[] buffer, int startOffsetInc, int endOffsetExc,
			boolean addLastChunk) {
		// Get the anchor list
		LinkedList<Integer> anchorList = getAnchors(buffer, startOffsetInc, endOffsetExc);
		if (anchorList == null)
			return 0;

		//
		// Get ready for the first chunk
		//
		int prevAnchor = startOffsetInc;
		Iterator<Integer> it = anchorList.iterator();
		Integer curAnchor = it.hasNext() ? it.next() : null;

		// On the first round we already have a "previous" and maybe a "current"

		while (true) {
			// If reached end of block or next anchor is too far
			if (curAnchor == null || (curAnchor - prevAnchor) > maxChunkSize) {
				// Chunk end at the largest allowed chunk or end of block
				int tempAnchor = Math.min(prevAnchor + maxChunkSize, endOffsetExc);

				int chunkLen = tempAnchor - prevAnchor;
				// If the last chunk is too small
				if (chunkLen < minChunkSize)
					return prevAnchor;
				// If the last chunk is not the maximum and the stream is not
				// fully processed
				if (chunkLen < maxChunkSize && !addLastChunk)
					return prevAnchor;
				// Signature
				long sha1 = calcSha1(buffer, prevAnchor, chunkLen);
				chunkList.add(chunkCode(sha1, chunkLen));
				prevAnchor = tempAnchor;
				continue;
			}

			// If the anchor is too close
			if ((curAnchor - prevAnchor) < minChunkSize) {
				// Skip the anchor and look for the next one
				curAnchor = it.hasNext() ? it.next() : null;
				continue;
			}

			// Here the anchor is fine: ok from first place or fixed

			// Chunk length
			int chunkLen = curAnchor - prevAnchor;
			// Calculate SHA-1 on the array from previous anchor (inclusive) to
			// the current (exclusive)
			long sha1 = calcSha1(buffer, prevAnchor, chunkLen);
			chunkList.add(chunkCode(sha1, chunkLen));

			// Next anchor
			prevAnchor = curAnchor;
			curAnchor = it.hasNext() ? it.next() : null;
		}
	}

	/**
	 * Similar to {@link #getChunks(Collection, byte[], int, int, boolean)} but it does not add duplicates, and it returns the number of overlapping bytes.
	 * <p>
	 * Get chunks in a given buffer, with limits on minimal or maximal chunk size. It assumes that the start offset is the beginning of a chunk but the last bytes
	 * are not.
	 * <p>
	 * When using for statistical analysis, beware that anchors cannot be found in the last 47 offsets, because of the sliding window size (48). The last chunk is
	 * added only if it si not too small and also asked specifically.
	 * 
	 * @param buffer
	 *            The given byte buffer.
	 * @param startOffsetInc
	 *            Start offset (0-based) of the search. The first anchor can be found in this offset, but involving the 47 bytes after it.
	 * @param endOffsetExc
	 *            Exclusive 0-based end offset of the search. The last anchor can be found only 48 bytes before that offset.
	 * @param addLastChunk
	 *            Use true only if this buffer is the end of a stream/file. Otherwise, the returned offset should be used for further processing. If true the last
	 *            chunk is added too, unless it is too small.
	 * @return Number of bytes overlapping other chunks from list and/or this buffer.
	 */
	public int getChunksOverlap(MessageDigest md, MapCounter<Long> stamps, byte[] buffer, int startOffsetInc,
			int endOffsetExc, boolean addLastChunk) {
		int result = 0;

		// Get the anchor list
		LinkedList<Integer> anchorList = getAnchors(buffer, startOffsetInc, endOffsetExc);
		if (anchorList == null)
			return 0;

		//
		// Get ready for the first chunk
		//
		int prevAnchor = startOffsetInc;
		Iterator<Integer> it = anchorList.iterator();
		Integer curAnchor = it.hasNext() ? it.next() : null;

		// On the first round we already have a "previous" and maybe a "current"

		while (true) {
			// If reached end of block or next anchor is too far
			if (curAnchor == null || (curAnchor - prevAnchor) > maxChunkSize) {
				// Chunk end at the largest allowed chunk or end of block
				int tempAnchor = Math.min(prevAnchor + maxChunkSize, endOffsetExc);

				int chunkLen = tempAnchor - prevAnchor;
				// If the last chunk is too small
				if (chunkLen < minChunkSize)
					return result;
				// If the last chunk is not the maximum and the stream is not
				// fully processed
				if (chunkLen < maxChunkSize && !addLastChunk)
					return result;
				// Signature
				long digest = calcMd(md, buffer, prevAnchor, chunkLen);
				long chunkCode = chunkCode(digest, chunkLen);
				// Check if stamp already exists (overlap) or add new
				long count = stamps.inc(chunkCode);
				if (count > 1)
					result += chunkLen;
				prevAnchor = tempAnchor;
				continue;
			}

			// If the anchor is too close
			if ((curAnchor - prevAnchor) < minChunkSize) {
				// Skip the anchor and look for the next one
				curAnchor = it.hasNext() ? it.next() : null;
				continue;
			}

			// Here the anchor is fine: ok from first place or fixed

			// Chunk length
			int chunkLen = curAnchor - prevAnchor;
			// Calculate SHA-1 on the array from previous anchor (inclusive) to
			// the current (exclusive)
			long digest = calcMd(md, buffer, prevAnchor, chunkLen);
			long chunkCode = chunkCode(digest, chunkLen);
			// Check if stamp already exists (overlap) or add new
			long count = stamps.inc(chunkCode);
			if (count > 1)
				result += chunkLen;

			// Next anchor
			prevAnchor = curAnchor;
			curAnchor = it.hasNext() ? it.next() : null;
		}
	}

	/**
	 * Similar to {@link #getChunks(Collection, byte[], int, int, boolean)} but it does not add duplicates, and it returns the number of overlapping bytes.
	 * <p>
	 * Get chunks in a given buffer, with limits on minimal or maximal chunk size. It assumes that the start offset is the beginning of a chunk but the last bytes
	 * are not.
	 * <p>
	 * When using for statistical analysis, beware that anchors cannot be found in the last 47 offsets, because of the sliding window size (48). The last chunk is
	 * added only if it si not too small and also asked specifically.
	 * 
	 * @param buffer
	 *            The given byte buffer.
	 * @param startOffsetInc
	 *            Start offset (0-based) of the search. The first anchor can be found in this offset, but involving the 47 bytes after it.
	 * @param endOffsetExc
	 *            Exclusive 0-based end offset of the search. The last anchor can be found only 48 bytes before that offset.
	 * @param addLastChunk
	 *            Use true only if this buffer is the end of a stream/file. Otherwise, the returned offset should be used for further processing. If true the last
	 *            chunk is added too, unless it is too small.
	 * @return Number of bytes overlapping other chunks from list and/or this buffer.
	 */
	public LinkedList<Long> getChunks(MessageDigest md, byte[] buffer, int startOffsetInc, int endOffsetExc,
			boolean addLastChunk) {
		LinkedList<Long> result = new LinkedList<Long>();

		// Get the anchor list
		LinkedList<Integer> anchorList = getAnchors(buffer, startOffsetInc, endOffsetExc);
		if (anchorList == null)
			return result;

		//
		// Get ready for the first chunk
		//
		int prevAnchor = startOffsetInc;
		Iterator<Integer> it = anchorList.iterator();
		Integer curAnchor = it.hasNext() ? it.next() : null;

		// On the first round we already have a "previous" and maybe a "current"

		while (true) {
			// If reached end of block or next anchor is too far
			if (curAnchor == null || (curAnchor - prevAnchor) > maxChunkSize) {
				// Chunk end at the largest allowed chunk or end of block
				int tempAnchor = Math.min(prevAnchor + maxChunkSize, endOffsetExc);

				int chunkLen = tempAnchor - prevAnchor;
				// If the last chunk is too small
				if (chunkLen < minChunkSize)
					return result;
				// If the last chunk is not the maximum and the stream is not
				// fully processed
				if (chunkLen < maxChunkSize && !addLastChunk)
					return result;
				// Signature
				long digest = calcMd(md, buffer, prevAnchor, chunkLen);
				long chunkCode = chunkCode(digest, chunkLen);
				result.add(chunkCode);
				prevAnchor = tempAnchor;
				continue;
			}

			// If the anchor is too close
			if ((curAnchor - prevAnchor) < minChunkSize) {
				// Skip the anchor and look for the next one
				curAnchor = it.hasNext() ? it.next() : null;
				continue;
			}

			// Here the anchor is fine: ok from first place or fixed

			// Chunk length
			int chunkLen = curAnchor - prevAnchor;
			// Calculate SHA-1 on the array from previous anchor (inclusive) to
			// the current (exclusive)
			long digest = calcMd(md, buffer, prevAnchor, chunkLen);
			long chunkCode = chunkCode(digest, chunkLen);

			// TODO temp
			if (chunkCode == 0x1e5d777a738728L) {
				String text = new String(buffer, prevAnchor, chunkLen);
				// System.out.println(String.format("----------\n%08x",
				// chunkCode));
				// System.out.println(text);
			}

			// Check if stamp already exists (overlap) or add new
			result.add(chunkCode);

			// Next anchor
			prevAnchor = curAnchor;
			curAnchor = it.hasNext() ? it.next() : null;
		}
	}

	/**
	 * Generate a 64-bits code that represents both the SHA-1 (partial) and length (limited).
	 * 
	 * @param sha1
	 *            Full SHA-1 hash.
	 * @param chunkLen
	 *            Chunk length (up to 512KB).
	 * @return 64-bits code that represents both the SHA-1 (partial) and length (limited).
	 */
	private static long chunkCode(long sha1, int chunkLen) {
		long result = ((chunkLen & CHUNK_LEN_MASK) << CHUNK_SHA1_BITS) | (CHUNK_SHA1_MASK & sha1);
		// System.out.println(String.format("%016x %,7d => %016x", sha1,
		// chunkLen, result));
		return result;
	}

	/**
	 * Convert chunk code, made of length and hash, to length.
	 * 
	 * @param chunkCode
	 *            The code, as generated by {@link #chunkCode(long, int)}.
	 * @return Chunk length, if not over the limit.
	 */
	public static long chunkCodeToLength(long chunkCode) {
		return (chunkCode >> CHUNK_SHA1_BITS) & CHUNK_LEN_MASK;
	}

	/**
	 * Print to console the full chunk list, each chunk in a separate line.
	 * 
	 * @param chunkList
	 *            List of chunks, each made of 64-bits, as generated by {@link #chunkCode(long, int)}.
	 */
	public static void printChunkList(List<Long> chunkList) {
		for (long curChunk : chunkList) {
			System.out.println(chunkToString(curChunk));
		}
	}

	/**
	 * 
	 * @param chunk
	 *            Chunk value as 64-bits that present both chunk length and partial SHA-1 signature.
	 * @return 14 characters for the SHA-1, space and 9 characters for length.
	 */
	public static String chunkToString(long chunk) {
		long sha1 = chunk & CHUNK_SHA1_MASK;
		int chunkLen = (int) ((chunk >> CHUNK_SHA1_BITS) & CHUNK_LEN_MASK);

		return String.format("%012x %,7d", sha1, chunkLen);
	}

	/**
	 * 
	 * @param chunk
	 *            Chunk value as 64-bits that present both chunk length and partial SHA-1 signature.
	 * @return Length of chunk.
	 */
	public static int chunkToLen(long chunk) {
		int chunkLen = (int) ((chunk >> CHUNK_SHA1_BITS) & CHUNK_LEN_MASK);

		return chunkLen;
	}

	/**
	 * Count number of anchors in a given buffer, without any limits on minimal or maximal chunk size.
	 * <p>
	 * When using for statistical analysis, beware that anchors cannot be found in the last 47 offsets, because of the sliding window size (48).
	 * 
	 * @param buffer
	 *            The given byte buffer.
	 * @param startOffsetInc
	 *            Start offset (0-based) of the search. The first anchor can be found in this offset, but involving the 47 bytes after it.
	 * @param endOffsetExc
	 *            Exclusive 0-based end offset of the search. The last anchor can be found only 48 bytes before that offset.
	 * @return Number of anchors found, or zero on input error (buffer length, offsets, etc).
	 */
	public int anchorCount(byte[] buffer, int startOffsetInc, int endOffsetExc) {
		// Sanity check
		if (buffer == null || startOffsetInc < 0 || endOffsetExc > buffer.length
				|| (endOffsetExc - startOffsetInc) < WINDOW_BYTES_LEN)
			return 0;

		int result = 0;

		long hash = 0;

		// Warm-up phase
		for (int i = 0; i < WINDOW_BYTES_LEN; i++) {
			hash = (hash << 1) ^ (0x00ffL & buffer[i]);
		}

		// Anchors
		for (int i = WINDOW_BYTES_LEN; i < buffer.length; i++) {
			// Check for anchor
			if ((hash & maskValue) == maskValue)
				result++;

			// Next hash
			hash = (hash << 1) ^ (0x00ffL & buffer[i]);
		}

		return result;
	}

	/**
	 * Get offsets of all anchors in a given buffer, without any limits on minimal or maximal chunk size.
	 * <p>
	 * The returned offsets are those of the window's oldest byte. Therefore, anchors cannot be found in the last 47 offsets, because of the sliding window size
	 * (48).
	 * 
	 * @param buffer
	 *            The given byte buffer.
	 * @param startOffsetInc
	 *            Start offset (0-based) of the search. The first anchor can be found in this offset, but involving the 47 bytes after it.
	 * @param endOffsetExc
	 *            Exclusive 0-based end offset of the search. The last anchor can be found only 48 bytes before that offset.
	 * @return Offset list of all the anchors found. The list will be empty if no anchors were found. Return null on input error (buffer length, offsets, etc).
	 */
	public LinkedList<Integer> getAnchors(byte[] buffer, int startOffsetInc, int endOffsetExc) {
		// Sanity check
		if (buffer == null || startOffsetInc < 0 || endOffsetExc > buffer.length
				|| (endOffsetExc - startOffsetInc) < WINDOW_BYTES_LEN)
			return null;

		LinkedList<Integer> result = new LinkedList<Integer>();

		long hash = 0;

		// Warm-up phase
		for (int i = 0; i < WINDOW_BYTES_LEN; i++) {
			hash = (hash << 1) ^ (0x00ffL & buffer[i]);
		}

		// Anchors
		for (int i = WINDOW_BYTES_LEN; i < endOffsetExc; i++) {
			// Check for anchor
			if ((hash & maskValue) == maskValue)
				result.add(i - WINDOW_BYTES_LEN);

			// Next hash
			hash = (hash << 1) ^ (0x00ffL & buffer[i]);
		}

		return result;
	}

	public int getMinChunkSize() {
		return minChunkSize;
	}

	public int getMaxChunkSize() {
		return maxChunkSize;
	}

	public int getAverageChunkLen() {
		return avgChunkSize;
	}

	/**
	 * Calculate hash for part of a buffer.
	 * <p>
	 * Does not contain a sanity check for input, to run faster.
	 * 
	 * @param buffer
	 *            Buffer that holds the data to hash. No matter where the position and limits are.
	 * @param offset
	 *            Offset in buffer's byte array (after the internal offset).
	 * @param len
	 *            How many bytes to put in the hash.
	 * @return Partial hash result which is the 64 LSB of the complete hash.
	 */
	private synchronized static long calcMd(MessageDigest md, byte[] buffer, int offset, int len) {
		initMd();

		// Do SHA-1
		byte[] mdArray;
		md.update(buffer, offset, len);
		mdArray = md.digest();

		long hash = ((0x00ffL & mdArray[0]) | ((0x00ffL & mdArray[1]) << 8) | ((0x00ffL & mdArray[2]) << 16)
				| ((0x00ffL & mdArray[3]) << 24) | ((0x00ffL & mdArray[4]) << 32) | ((0x00ffL & mdArray[5]) << 40)
				| ((0x00ffL & mdArray[6]) << 48) | ((0x00ffL & mdArray[7]) << 56));

		return hash;
	}

	/**
	 * Calculate SHA-1 for part of a buffer.
	 * <p>
	 * Does not contain a sanity check for input, to run faster.
	 * 
	 * @param buffer
	 *            Buffer that holds the data to hash. No matter where the position and limits are.
	 * @param offset
	 *            Offset in buffer's byte array (after the internal offset).
	 * @param len
	 *            How many bytes to put in the hash.
	 * @return Partial hash result which is the 64 LSB of the complete SHA-1 (160 bits).
	 */
	private synchronized static long calcSha1(byte[] buffer, int offset, int len) {
		return calcMd(mdSha1, buffer, offset, len);
	}

	/**
	 * Calculate MD5 for part of a buffer.
	 * <p>
	 * Does not contain a sanity check for input, to run faster.
	 * 
	 * @param buffer
	 *            Buffer that holds the data to hash. No matter where the position and limits are.
	 * @param offset
	 *            Offset in buffer's byte array (after the internal offset).
	 * @param len
	 *            How many bytes to put in the hash.
	 * @return Partial hash result which is the 64 LSB of the complete MD5 (128 bits).
	 */
	private synchronized static long calcMd5(byte[] buffer, int offset, int len) {
		return calcMd(mdMd5, buffer, offset, len);
	}

	public List<Long> getFileChunks(String fileName, ByteBuffer buffer) {
		LinkedList<Long> curChunkList = new LinkedList<Long>();
		long curOffset = 0;

		// File blocks loop
		while (true) {
			buffer.position(0);
			buffer.limit(buffer.capacity());
			int readBytes = FileUtils.readBlock(buffer, fileName, curOffset, null);
			if (readBytes == 0)
				break;
			// Add the chunks to the list
			int offsetNext = this.getChunks(curChunkList, buffer.array(), buffer.arrayOffset(),
					buffer.arrayOffset() + readBytes, false);

			if (readBytes < buffer.capacity())
				break;

			// Next file offset
			curOffset += offsetNext;
		}

		return curChunkList;
	}
}
