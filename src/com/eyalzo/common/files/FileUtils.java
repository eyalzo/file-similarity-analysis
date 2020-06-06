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
package com.eyalzo.common.files;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Read and write methods to data files.
 * 
 * @author Eyal Zohar
 */
public class FileUtils {
	/**
	 * Write received block to physical file on disk.
	 * 
	 * @param fileName
	 *            Full path of existing file on disk.
	 * @param startOffset
	 *            0-based start position in file, inclusive.
	 * @param buffer
	 *            Buffer holding the block itself, from position(), meaning that its remaining() is the length of data to write.
	 * @param log
	 * @return False if file does not exist or failed to write all bytes from any other reason.
	 */
	public static boolean writeBlock(String fileName, long startOffset, ByteBuffer buffer, Logger log) {
		RandomAccessFile randomAccessFile = null;
		File file = new File(fileName);

		//
		// File is supposed to be on disk already, since the file was used for
		// the first time
		//
		if (!file.exists()) {
			if (log != null)
				log.log(Level.WARNING, "Cannot write file \"" + fileName + "\" that does not exist");
			return false;
		}

		//
		// Make sure that position and length match the file size
		//
		if (startOffset + buffer.remaining() > file.length()) {
			if (log != null)
				log.log(Level.SEVERE,
						"Wrong position and/or length when trying to write file \"" + fileName + "\", file length "
								+ file.length() + ", offset " + startOffset + ", data length " + buffer.remaining());
			return false;
		}

		//
		// Create the file descriptor
		//
		try {
			// It can fail if file does not exist or there is a security problem
			randomAccessFile = new RandomAccessFile(file, "rw");
		} catch (Exception e) {
			if (log != null)
				log.log(Level.WARNING, "Failed to open file \"" + fileName + "\" for write:\n" + e);
			return false;
		}

		//
		// Move writing head to the right position
		//
		try {
			randomAccessFile.seek(startOffset);
		} catch (Exception e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to set position when trying to write file \"" + fileName
						+ "\", file length " + file.length() + ", offset " + startOffset + ":\n{3}" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return false;
		}

		WritableByteChannel channel = randomAccessFile.getChannel();

		//
		// Write the data
		//
		int dataLength = buffer.remaining();
		int writtenBytes = 0;
		try {
			writtenBytes = channel.write(buffer);
		} catch (IOException e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to write data to file \"" + fileName + "\", file length " + file.length()
						+ ", offset " + startOffset + ", data length " + dataLength + ":\n" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return false;
		}

		//
		// Close the file
		//
		try {
			randomAccessFile.close();
		} catch (Exception e2) {
		}

		//
		// Check if all bytes were written
		//
		if (writtenBytes != dataLength) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to write data to file \"" + fileName + "\", wrote only " + writtenBytes
						+ " instead of " + dataLength);
			return false;
		}

		return true;
	}

	/**
	 * Write the given string buffer to a new file.
	 * 
	 * @param fileName
	 *            Full path of existing file on disk.
	 * @param buffer
	 *            Buffer holding the entire block to be written.
	 * @param log
	 * @return False if file was failed to write all bytes from any.
	 */
	public static boolean writeStringBuffer(String fileName, StringBuffer buffer, Logger log) {
		return writeStringBuffer(fileName, buffer, false, log);
	}

	/**
	 * Write the given string buffer to a new file.
	 * 
	 * @param fileName
	 *            Full path of existing file on disk.
	 * @param buffer
	 *            Buffer holding the entire block to be written.
	 * @param append
	 *            If true, will append to end of file.
	 * @param log
	 * @return False if file was failed to write all bytes from any.
	 */
	public static boolean writeStringBuffer(String fileName, StringBuffer buffer, boolean append, Logger log) {
		File file = new File(fileName);

		//
		// Write the data
		//
		BufferedWriter out;
		try {
			out = new BufferedWriter(new FileWriter(fileName, append));
		} catch (IOException e1) {
			if (log != null)
				log.log(Level.SEVERE,
						"Failed to create the file \"" + fileName + "\", file length " + file.length() + ":\n" + e1);
			return false;
		}

		try {
			out.write(buffer.toString());
		} catch (IOException e) {
			if (log != null)
				log.log(Level.SEVERE,
						"Failed to write data to file \"" + fileName + "\", file length " + file.length() + ":\n" + e);
			try {
				out.close();
			} catch (Exception e2) {
			}
			return false;
		}

		//
		// Close the file
		//
		try {
			out.close();
		} catch (Exception e2) {
		}

		return true;
	}

	/**
	 * Read one block from data file.
	 * 
	 * @param fileName
	 * @param startOffset
	 *            0-based start position in file, inclusive.
	 * @param length
	 *            If positive, the length of the block to read. Otherwise it will read to end-of-file.
	 * @param log
	 * @return Null if failed to read form some reason.
	 */
	public static ByteBuffer readBlock(String fileName, long startOffset, int length, ByteOrder byteOrder, Logger log) {
		File file = new File(fileName);

		//
		// File is supposed to be on disk
		//
		if (!file.isFile()) {
			if (log != null)
				log.log(Level.WARNING, "Cannot read file {0}, that does not exist", fileName);
			return null;
		}

		//
		// Make sure that position and length match the file size
		//
		if (length <= 0) {
			length = (int) (file.length() - startOffset);
		} else if (startOffset + length > file.length()) {
			if (log != null)
				log.log(Level.SEVERE, "Wrong position and/or length when trying to read file \"" + fileName
						+ "\", file length " + file.length() + ", offset " + startOffset + ", data length " + length);
			return null;
		}

		RandomAccessFile randomAccessFile;
		//
		// Create the file descriptor
		//
		try {
			// It can fail if file does not exist or there is a security problem
			randomAccessFile = new RandomAccessFile(file, "rw");
		} catch (Exception e) {
			if (log != null)
				log.log(Level.WARNING, "Failed to open file \"" + fileName + "\" for read:\n" + e);
			return null;
		}

		//
		// Move writing head to the right position
		//
		try {
			randomAccessFile.seek(startOffset);
		} catch (Exception e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to set position when trying to read file \"" + fileName
						+ "\", file length " + file.length() + ", offset " + startOffset + ":\n" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return null;
		}

		// Allocate new buffer
		ByteBuffer buffer = ByteBuffer.allocate(length).order(byteOrder);

		ReadableByteChannel channel = randomAccessFile.getChannel();

		//
		// Read the data
		//
		int readBytes = 0;
		try {
			readBytes = channel.read(buffer);
		} catch (IOException e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to read data from \"" + fileName + "\", file length " + file.length()
						+ ", offset " + startOffset + ", data length " + length + ":\n" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return null;
		}

		//
		// Close the file
		//
		try {
			randomAccessFile.close();
		} catch (Exception e2) {
		}

		//
		// Check if all bytes were written
		//
		if (readBytes != length) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to read data from \"" + fileName + "\", read only " + readBytes
						+ " instead of " + length);
			return null;
		}

		buffer.flip();

		return buffer;
	}

	/**
	 * Read an entire file to new allocated byte array.
	 * 
	 * @param fileName
	 *            Full path of file to read.
	 * @param log
	 *            Optional log.
	 * @return Null if failed to read form some reason, or a full array which its size is exactly the number of successfully read bytes.
	 */
	public static byte[] readFileToByteArray(String fileName, Logger log) {
		return readBlockToByteArray(fileName, 0, 0, log);
	}

	/**
	 * Read one block from data file.
	 * 
	 * @param fileName
	 *            Full path of file to read.
	 * @param startOffset
	 *            0-based start position in file, inclusive.
	 * @param length
	 *            If positive, the length of the block to read. Otherwise it will read to end-of-file.
	 * @param log
	 *            Optional log.
	 * @return Null if failed to read form some reason, or a full array which its size is exactly the number of successfully read bytes.
	 */
	public static byte[] readBlockToByteArray(String fileName, long startOffset, int length, Logger log) {
		File file = new File(fileName);

		//
		// File is supposed to be on disk
		//
		if (!file.isFile()) {
			if (log != null)
				log.log(Level.WARNING, "Cannot read file \"" + fileName + "\", that does not exist");
			return null;
		}

		//
		// Make sure that position and length match the file size
		//
		if (length <= 0) {
			long longLength = file.length() - startOffset;
			length = (longLength > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) longLength;
		} else if (startOffset + length > file.length()) {
			if (log != null)
				log.log(Level.SEVERE, "Wrong position and/or length when trying to read file \"" + fileName
						+ "\", file length " + file.length() + ", offset " + startOffset + ", data length " + length);
			return null;
		}

		RandomAccessFile randomAccessFile;
		//
		// Create the file descriptor
		//
		try {
			// It can fail if file does not exist or there is a security problem
			randomAccessFile = new RandomAccessFile(file, "r");
		} catch (Exception e) {
			if (log != null)
				log.log(Level.WARNING, "Failed to open file \"" + fileName + "\" for read:\n" + e);
			return null;
		}

		//
		// Allocate new buffer
		//
		byte[] buffer = new byte[length];

		//
		// Read the data
		//
		try {
			// Seek first and don't call readFull(...,int,...) that does not
			// allow long offsets
			randomAccessFile.seek(startOffset);
			randomAccessFile.readFully(buffer);
		} catch (IOException e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to read data from \"" + fileName + "\", file length " + file.length()
						+ ", offset " + startOffset + ", data length " + length + ":\n" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return null;
		}

		//
		// Close the file
		//
		try {
			randomAccessFile.close();
		} catch (Exception e2) {
		}

		return buffer;
	}

	/**
	 * Read one block from data file.
	 * 
	 * @param buffer
	 *            Buffer ready for reading from position to limit. Need to set the limit before call. On successful return it has the position and limit ready for
	 *            reading.
	 * @param fileName
	 * @param startOffset
	 *            0-based start position in file, inclusive.
	 * @param log
	 *            Optional.
	 * @return Number of read bytes or 0 on error: missing file, end of file, buffer is not large enough, etc.
	 */
	public static int readBlock(ByteBuffer buffer, String fileName, long startOffset, Logger log) {
		File file = new File(fileName);

		//
		// File is supposed to be on disk
		//
		if (!file.exists()) {
			if (log != null)
				log.log(Level.WARNING, "Cannot read file {0}, that does not exist", fileName);
			return 0;
		}

		//
		// Make sure that position and length match the file size
		//
		if (startOffset + buffer.remaining() > file.length()) {
			int length = (int) (file.length() - startOffset);
			if (length <= 0) {
				if (log != null)
					log.log(Level.SEVERE,
							"Wrong position and/or length when trying to read file \"" + fileName + "\", file length "
									+ file.length() + ", offset " + startOffset + ", data length " + length);
				return 0;
			}

			buffer.limit(buffer.position() + length);
		}

		RandomAccessFile randomAccessFile;
		//
		// Create the file descriptor
		//
		try {
			// It can fail if file does not exist or there is a security problem
			randomAccessFile = new RandomAccessFile(file, "rw");
		} catch (Exception e) {
			if (log != null)
				log.log(Level.WARNING, "Failed to open file \"" + fileName + "\" for read:\n" + e);
			return 0;
		}

		//
		// Move writing head to the right position
		//
		try {
			randomAccessFile.seek(startOffset);
		} catch (Exception e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to set position when trying to read file \"" + fileName
						+ "\", file length " + file.length() + ", offset " + startOffset + ":\n" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return 0;
		}

		ReadableByteChannel channel = randomAccessFile.getChannel();

		//
		// Read the data
		//
		int readBytes = 0;
		try {
			readBytes = channel.read(buffer);
		} catch (IOException e) {
			if (log != null)
				log.log(Level.SEVERE, "Failed to read data from \"" + fileName + "\", file length " + file.length()
						+ ", offset " + startOffset + ", data length " + buffer.remaining() + ":\n" + e);
			try {
				randomAccessFile.close();
			} catch (Exception e2) {
			}
			return 0;
		}

		//
		// Close the file
		//
		try {
			randomAccessFile.close();
		} catch (Exception e2) {
		}

		buffer.limit(buffer.position());
		buffer.position(buffer.position() - readBytes);

		return readBytes;
	}

	/**
	 * Set data filename, and create it if needed and asked to.
	 * 
	 * @param downloadsDirName
	 * @return True if file already exists.
	 */
	public static boolean initDataFile(String dirName, String fileName, long fileSize, boolean createFileIfNotExists) {
		String dataFileName = dirName + File.separatorChar + fileName;
		return initDataFile(dataFileName, fileSize, createFileIfNotExists);
	}

	public static boolean initDataFile(String dataFileName, long fileSize, boolean createFileIfNotExists) {
		File file = new File(dataFileName);

		//
		// If file already exists we can safely return now
		//
		if (file.exists()) {
			return true;
		}

		if (createFileIfNotExists) {
			return createDataFile(file, fileSize);
		}
		return false;
	}

	private static boolean createDataFile(File file, long fileSize) {
		RandomAccessFile randomAccessFile = null;
		//
		// Try to create the new file
		//
		try {
			file.createNewFile();
			randomAccessFile = new RandomAccessFile(file, "rw");
		} catch (IOException e) {
			return false;
		}

		//
		// Set the file size
		//
		try {
			randomAccessFile.setLength(fileSize);
		} catch (Exception e) {
			try {
				randomAccessFile.close();
			} catch (Exception e1) {
			}
			return false;
		}

		try {
			randomAccessFile.close();
		} catch (Exception e1) {
		}
		return true; // File was created successfully
	}

	/**
	 * Creates a directory and all its parents if not already exist.
	 * 
	 * @param dirName
	 *            The directory to check and create if needed
	 * @param log
	 *            Optional logger for warnings.
	 * @return True if the directory existed or successfully created, otherwise False
	 */
	public static boolean createDir(String dirName, Logger log) {
		File newFolder = new File(dirName);
		//
		// Even though the first thing that mkdirs() does is check the same,
		// i add this condition for debug
		//
		if (newFolder.exists())
			return true;

		try {
			newFolder.mkdirs();
			if (log != null)
				log.log(Level.INFO, "Created dir {0} and all its missing parent dirs (if any)", dirName);
			return true;
		} catch (Exception e) {
			if (log != null && log.isLoggable(Level.WARNING)) {
				if (log != null)
					log.log(Level.WARNING, "Error when trying to create directory \"" + dirName + "\":\n" + e);
			}
			return false;
		}
	}

	public static long getFileSize(String fileName, Logger log) {
		File file = new File(fileName);

		//
		// File is supposed to be on disk already, since the file was used for
		// the first time
		//
		if (!file.exists()) {
			if (log != null)
				log.log(Level.WARNING, "File \"" + fileName + "\" not found, so size is zero");
			return 0;
		}

		return file.length();
	}

	/**
	 * 
	 * @param dirName
	 * @param minFileSize
	 * @param maxFileSize
	 *            If positive, return only files not larger than this size in bytes.
	 * @return List of files in dir, sorted by name as string. May return null of dir name is empty of do not describe an existing directory.
	 */
	public static List<String> getDirFileListSorted(String dirName, long minFileSize, long maxFileSize) {
		// Sanity check
		if (dirName == null)
			return null;

		// Make sure that this is an existing directory
		File file = new File(dirName);
		if (!file.isDirectory())
			return null;

		dirName = file.getAbsolutePath();

		// Get all names
		LinkedList<String> list = new LinkedList<String>();
		String[] fileNames = file.list();

		// This should not happen after we already verified that the directory
		// exists
		if (fileNames == null)
			return null;

		for (String curName : fileNames) {
			String fullPath = dirName + File.separatorChar + curName;

			// Make sure that the entry is for an existing file
			File curFile = new File(fullPath);
			if (curFile == null || !curFile.isFile())
				continue;

			// File size
			if (curFile.length() < minFileSize || (maxFileSize > 0 && curFile.length() > maxFileSize))
				continue;

			// Add full path to list
			list.add(fullPath);
		}

		// Sort the list (slower with full path, but...)
		Collections.sort(list);

		return list;
	}

	public static List<String> getDirSubdirListSorted(String dirName) {
		// Sanity check
		if (dirName == null)
			return null;

		// Make sure that this is an existing directory
		File file = new File(dirName);
		if (!file.isDirectory())
			return null;

		dirName = file.getAbsolutePath();

		// Get all names
		LinkedList<String> list = new LinkedList<String>();
		String[] fileNames = file.list();

		// This should not happen after we already verified that the directory
		// exists
		if (fileNames == null)
			return null;

		for (String curName : fileNames) {
			String fullPath = dirName + File.separatorChar + curName;

			// Make sure that the entry is for an existing file
			File curFile = new File(fullPath);
			if (curFile == null || !curFile.isDirectory())
				continue;

			// Add full path to list
			list.add(fullPath);
		}

		// Sort the list (slower with full path, but...)
		Collections.sort(list);

		return list;
	}

	/**
	 * Read text file as text lines for easier processing.
	 * 
	 * @param fileName
	 *            Full path of file name to read.
	 * @return List of strings read from file (without the ending newline). Never null.
	 */
	public static LinkedList<String> readFileToTextLines(String fileName, Logger log) {
		LinkedList<String> result = new LinkedList<String>();

		try {
			BufferedReader in = new BufferedReader(new FileReader(fileName));
			String str;
			while ((str = in.readLine()) != null) {
				result.add(str);
			}
			in.close();
		} catch (IOException e) {
			return result;
		}

		return result;
	}

	/**
	 * Read file to text lines, and replace a given line with another. Can also just add a new line, or just remove an old one.
	 * 
	 * @param fileName
	 *            File name.
	 * @param oldLine
	 *            Optional old line to remove. Can be null or empty if just want to add a new line.
	 * @param updatedLine
	 *            Optional new line. If null the method will just remove the old one.
	 * @param log
	 *            Optional.
	 * @return True the old line was removed (if asked to) and the new one written (if asked to).
	 */
	public static boolean updateFileTextLine(String fileName, String oldLine, String updatedLine,
			boolean allowDuplicates, Logger log) {
		// Read the current file
		LinkedList<String> lines = readFileToTextLines(fileName, log);
		if (lines.isEmpty())
			return false;

		// Remove the old line, if asked to
		if (oldLine != null && !oldLine.isEmpty()) {
			// Check if exists and remove it
			if (!lines.remove(oldLine))
				return false;
		}

		// Add the new line, if asked to
		if (updatedLine != null && !updatedLine.isEmpty()) {
			// Add only if duplicates are allowed or the line is fresh
			if (allowDuplicates || !lines.contains(updatedLine))
				lines.add(updatedLine);
		}

		return writeTextLines(fileName, lines, log);
	}

	public static boolean writeTextLines(String fileName, List<String> textLines, Logger log) {
		StringBuffer buffer = new StringBuffer();

		for (String curLine : textLines) {
			buffer.append(curLine);
			buffer.append("\r\n");
		}

		return writeStringBuffer(fileName, buffer, log);
	}

	/**
	 * @param dirName
	 *            Directory name.
	 * @return True of the directory name is legal and directory exists.
	 */
	public static boolean dirExists(String dirName) {
		if (dirName == null || dirName.isEmpty())
			return false;

		File file = new File(dirName);
		return file.isDirectory();
	}

	/**
	 * @param fileName
	 *            File name.
	 * @return True of the directory name is legal and directory exists.
	 */
	public static boolean fileExists(String fileName) {
		if (fileName == null || fileName.isEmpty())
			return false;

		File file = new File(fileName);
		return file.isFile();
	}

	/**
	 * Get absolute path of current directory or file in the current directory. If the given name is already an absolute path then it will be returned as is.
	 * 
	 * @param fileName
	 *            File name or file name with relative path. If empty or null the returned string is the current directory without an ending path separator.
	 * @return Full path (absolute), or null on error.
	 */
	public static String getCurrentDirFile(String fileName) {
		if (fileName == null)
			return null;

		// Check if absolute pach already
		if (File.separatorChar == '/') {
			// Linux
			if (fileName.startsWith("/"))
				return fileName;
		} else {
			if (fileName.matches("[a-zA-Z]:\\\\.*"))
				return fileName;
		}

		String curDir = System.getProperty("user.dir");
		// If the current dir has to be returned
		if (fileName == null || fileName.isEmpty())
			return curDir;
		File file = new File(curDir, fileName);
		return file.getAbsolutePath();
	}

	/**
	 * Replaces all potentially forbidden characters (/\*?:) in file name with a single underline for each.
	 * 
	 * @param fileName
	 *            A given file name (not path).
	 * @return Never null. Each forbidden character is replaced with a single underline.
	 */
	public static String replaceFileNameForbiddenChars(String fileName) {
		if (fileName == null || fileName.isEmpty())
			return "";

		return fileName.replace(':', '_').replace('/', '_').replace('*', '_').replace('?', '_').replace('\\', '_');
	}

	/**
	 * @param fileName
	 *            Full path of file to delete.
	 * @return Same as {@link File#delete()}, only with a preliminary sanity check.
	 */
	public static boolean deleteFile(String fileName) {
		if (fileName == null || fileName.isEmpty())
			return false;

		File file = new File(fileName);
		return file.delete();
	}
}
