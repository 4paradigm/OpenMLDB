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

public class QueryCommand extends CommandPacket {

	private final String query;
	private final String database;
	private final String userName;
	private final byte[] scramble411;

	public QueryCommand(int sequenceId, String query, String database, String userName, byte[] scramble411) {
		super(sequenceId, Command.COM_QUERY);
		this.query = query;
		this.database = database;
		this.userName = userName;
		this.scramble411 = scramble411;
	}

	public String getQuery() {
		return query;
	}

	public String getDatabase() {
		return database;
	}

	public String getUserName() {
		return userName;
	}

	public byte[] getScramble411() {
		return scramble411;
	}
}
