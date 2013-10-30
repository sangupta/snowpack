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

package com.sangupta.snowpack.domain;

import java.util.ArrayList;
import java.util.List;


/**
 * Holds the meta information on the parent Snowpack object.
 * 
 * @author sangupta
 *
 */
public class SnowpackMeta {
	
	/**
	 * The number of chunks in the store
	 */
	public int numChunks;

	/**
	 * The complete list of chunks and their related information
	 */
	public final List<ChunkInfo> chunks = new ArrayList<ChunkInfo>();
	
	/**
	 * Get the last chunk from the list of chunks.
	 * 
	 * @return
	 */
	public ChunkInfo getLastChunk() {
		if(this.numChunks == 0) {
			return null;
		}
		
		if(chunks.size() < this.numChunks) {
			return null;
		}
		
		return this.chunks.get(this.numChunks - 1);
	}
	
	/**
	 * Update chunks internally.
	 * 
	 * @param chunks
	 */
	public void updateChunks(List<ChunkInfo> chunks) {
		if(chunks == null) {
			throw new IllegalArgumentException("Chunks to be set cannot be null");
		}
		
		// add given chunks
		this.chunks.clear();
		this.chunks.addAll(chunks);
		
		// set num chunks
		this.numChunks = chunks.size();
	}
	
}
