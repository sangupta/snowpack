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

import java.nio.ByteBuffer;

/**
 * Holds metadata information about one flake.
 * 
 * @author sangupta
 *
 */
public class FlakeMetadata {

	/**
	 * The unique flake name or identifier in the system
	 */
	public String flakeName;
	
	/**
	 * The length of the flake
	 */
	public int length;
	
	/**
	 * The chunk in which this flake is stored
	 */
	public int chunk;
	
	/**
	 * The offset of the flake in the snowpack chunk
	 */
	public long offset;
	
	/**
	 * The length of the header data in the pack - this can be skipped directly to read chunk data
	 */
	public int headerLength;
	
	/**
	 * Constructor
	 * 
	 * @param name
	 * @param length
	 * @param chunk
	 * @param offset
	 * @param headerLength
	 */
	public FlakeMetadata(String name, int length, int chunk, long offset, int headerLength) {
		this.flakeName = name;
		this.length = length;
		this.chunk = chunk;
		this.offset = offset;
		this.headerLength = headerLength;
	}
	
	/**
	 * Convenience constructor
	 * 
	 * @param flake
	 * @param chunk
	 * @param pointer
	 */
	public FlakeMetadata(Flake flake, int chunk, long pointer, int headerSize) {
		this.flakeName = flake.flakeName;
		this.length = flake.length;
		this.chunk = chunk;
		this.offset = pointer;
		this.headerLength = headerSize;
	}
	
	/**
	 * Construct and object from previously serialized version.
	 * 
	 * @param flakeName
	 * @param bytes
	 */
	public FlakeMetadata(String flakeName, byte[] bytes) {
		this.flakeName = flakeName;
		this.fromBytes(bytes);
	}

	/**
	 * Return the flake metadata as bytes.
	 * 
	 * @return
	 */
	public byte[] asBytes() {
		ByteBuffer buffer = ByteBuffer.allocate(20); // 2 ints and 2 longs - 2 * 32 + 2 * 64 = 192 bits = 24 bytes
		buffer.putInt(this.length);
		buffer.putInt(this.chunk);
		buffer.putLong(this.offset);
		buffer.putInt(this.headerLength);
		
		return buffer.array();
	}

	/**
	 * Initialize this object using the given bytes.
	 * 
	 * @param flakeName
	 * @param bytes
	 */
	private void fromBytes(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		this.length = buffer.getInt();
		this.chunk = buffer.getInt();
		this.offset = buffer.getLong();
		this.headerLength = buffer.getInt();
	}

}
