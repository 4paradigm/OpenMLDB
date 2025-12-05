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

/**
 * The MySQL client/server capability flags.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/status-flags.html">Server Status Flags Reference
 * Documentation</a>
 */
public enum ServerStatusFlag {
	IN_TRANSACTION,
	AUTO_COMMIT,
	MORE_RESULTS_EXIST,
	NO_GOOD_INDEX_USED,
	CURSOR_EXISTS,
	LAST_ROW_SENT,
	DATABASE_DROPPED,
	NO_BACKSLASH_ESCAPES,
	METADATA_CHANGED,
	QUERY_WAS_SLOW,
	PREPARED_STATEMENT_OUT_PARAMS,
	IN_READONLY_TRANSACTION,
	SESSION_STATE_CHANGED,
	UNKNOWN_13,
	UNKNOWN_14,
	UNKNOWN_15
}
