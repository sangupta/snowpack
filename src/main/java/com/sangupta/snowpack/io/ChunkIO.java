/**
 *
 * snowpack - Pack flakes in chunks
 * Copyright (c) 2013, Sandeep Gupta
 * 
 * http://www.sangupta/projects/snowpack
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.sangupta.snowpack.io;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sangupta.snowpack.Snowpack;
import com.sangupta.snowpack.domain.ChunkInfo;
import com.sangupta.snowpack.domain.Flake;
import com.sangupta.snowpack.domain.FlakeMetadata;

/**
 * Abstract IO class which chunk readers and chunk writers can implement for
 * providing their respective functionality.
 * 
 * @author sangupta
 *
 */
public abstract class ChunkIO implements Closeable {

	/**
	 * Reference to the chunk file over which we are going to write
	 */
	protected final File chunkFile;
	
	/**
	 * The index of this chunk in the {@link Snowpack}.
	 */
	protected final int chunkIndex;
	
	/**
	 * The actual {@link RandomAccessFile} handler that handles this file
	 */
	protected final RandomAccessFile handler;
	
	/**
	 * Keeps track of number of files in this chunk
	 */
	protected volatile int numFiles;
	
	/**
	 * The lock to syncrhonize multiple threads writing the same file
	 */
	protected final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	/**
	 * 
	 * @param config
	 * @param chunkFile
	 * @param chunkIndex
	 * @param mode
	 * @throws FileNotFoundException 
	 */
	protected ChunkIO(File chunkFile, int chunkIndex, String mode) throws FileNotFoundException {
		this.chunkFile = chunkFile;
		this.chunkIndex = chunkIndex;
		
		this.handler = new RandomAccessFile(this.chunkFile, mode);
	}
	
	/**
	 * Return the current chunk's index.
	 * 
	 * @return
	 */
	public final int getChunkIndex() {
		return this.chunkIndex;
	}

	/**
	 * Return the information on this chunk.
	 * 
	 * @return
	 */
	public ChunkInfo getChunkInfo() {
		ChunkInfo info = new ChunkInfo();
		
		info.chunkID = this.chunkIndex;
		info.numFiles = this.numFiles;
		info.writePointer = -1;
		
		return info;
	}

	/**
	 * Read a flake from the underlying handler. The method is abstract to make sure
	 * that the chunk readers and chunk writers can have a different impleemtations as
	 * writers will read from memory only, whereas readers will use a global cache
	 * with underlying disk read.
	 * 
	 * @param flakeMetadata
	 * @return
	 */
	public abstract Flake readFlake(FlakeMetadata flakeMetadata) throws IOException;
	
}
