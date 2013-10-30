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

/**
 * Stores information about one given chunk. A chunk is a
 * collection of flakes stored together.
 * 
 * @author sangupta
 *
 */
public class ChunkInfo implements Comparable<ChunkInfo> {

	/**
	 * The chunk ID for which this information is kept
	 * 
	 */
	public int chunkID;
	
	/**
	 * Number of files inside the chunk
	 */
	public int numFiles;
	
	/**
	 * The location of the write pointer in the file
	 */
	public long writePointer;
	
	/**
	 * Generate string representation of object
	 */
	@Override
	public String toString() {
		return "[Chunk id:" + this.chunkID + ", files:" + this.numFiles + "]";
	}

	/**
	 * Check if two {@link ChunkInfo} objects are equal or not.
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		
		if(this == obj) {
			return true;
		}
		
		if(!(obj instanceof ChunkInfo)) {
			return false;
		}
		
		return this.chunkID == ((ChunkInfo) obj).chunkID;
	}
	
	/**
	 * Compute the hash-code for this object.
	 */
	@Override
	public int hashCode() {
		return Integer.valueOf(this.chunkID).hashCode();
	}

	/**
	 * Compare two {@link ChunkInfo} objects to sort them out.
	 */
	@Override
	public int compareTo(ChunkInfo o) {
		if(o == null) {
			return -1;
		}
		
		return 0 - (this.chunkID - o.chunkID);
	}
	
}
