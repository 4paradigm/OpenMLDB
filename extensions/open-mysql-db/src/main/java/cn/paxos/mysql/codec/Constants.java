/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql.codec;

public interface Constants {

	int NUL_BYTE = 0x00;

	int RESPONSE_OK = 0x00;
	int RESPONSE_EOF = 0xfe;
	int RESPONSE_ERROR = 0xff;

	int MINIMUM_SUPPORTED_PROTOCOL_VERSION = 10;

	int SQL_STATE_SIZE = 6;

	// Handshake constants
	int AUTH_PLUGIN_DATA_PART1_LEN = 8;
	int AUTH_PLUGIN_DATA_PART2_MIN_LEN = 13;
	int AUTH_PLUGIN_DATA_MIN_LEN = AUTH_PLUGIN_DATA_PART1_LEN + AUTH_PLUGIN_DATA_PART2_MIN_LEN;
	int HANDSHAKE_RESERVED_BYTES = 10;

	// Auth plugins
	String DEFAULT_AUTH_PLUGIN_NAME = "mysql_native_password";
	String CACHING_SHA2_PASSWORD = "caching_sha2_password";

	// Changed from 1MB to 10MB.
	int DEFAULT_MAX_PACKET_SIZE = 10485760;
}
