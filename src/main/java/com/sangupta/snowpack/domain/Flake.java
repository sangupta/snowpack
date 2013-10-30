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
 * Holds information about one flake in the snowpack chunks. A flake
 * is one file that is stored inside the snowpack.
 * 
 * @author sangupta
 *
 */
public class Flake {

	/**
	 * The file name or the flake name
	 */
	public String flakeName;
	
	/**
	 * length of the flake/file
	 */
	public int length;
	
	/**
	 * Timestamp when this flake/file was created in the snowpack
	 */
	public long created;
	
	/**
	 * Bytes for this flake/file
	 */
	public byte[] bytes;
	
	/**
	 * Default constructor
	 */
	public Flake() {
		
	}
	
	/**
	 * Convenience constructor.
	 * 
	 * @param bytes
	 */
	public Flake(String name, int length, long created, byte[] bytes) {
		this.flakeName = name;
		this.length = length;
		this.created = created;
		this.bytes = bytes;
	}
}
