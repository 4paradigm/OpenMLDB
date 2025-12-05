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

import java.util.Optional;

public enum Command {
	// Old (since MySQL 3.20) commands
	COM_SLEEP(0x00),
	COM_QUIT(0x01),
	COM_INIT_DB(0x02),
	COM_QUERY(0x03),
	COM_FIELD_LIST(0x04),
	COM_CREATE_DB(0x05),
	COM_DROP_DB(0x06),
	COM_REFRESH(0x07),
	COM_SHUTDOWN(0x08),
	COM_STATISTICS(0x09),
	COM_PROCESS_INFO(0x0a),
	COM_CONNECT(0x0b),
	COM_PROCESS_KILL(0x0c),
	COM_DEBUG(0x0d),
	COM_PING(0x0e),
	COM_TIME(0x0f),
	COM_DELAYED_INSERT(0x10),
	COM_CHANGE_USER(0x11),
	COM_RESET_CONNECTION(0x1f),
	COM_DAEMON(0x1d),

	// Prepared statements
	COM_STMT_PREPARE(0x16),
	COM_STMT_SEND_LONG_DATA(0x18),
	COM_STMT_EXECUTE(0x17),
	COM_STMT_CLOSE(0x19),
	COM_STMT_RESET(0x1a),

	// Stored procedures
	COM_SET_OPTION(0x1b),
	COM_STMT_FETCH(0x1c),

	// Replication protocol
	COM_BINLOG_DUMP(0x12),
	COM_BINLOG_DUMP_GTID(0x1e),
	COM_TABLE_DUMP(0x13),
	COM_CONNECT_OUT(0x14),
	COM_REGISTER_SLAVE(0x15);

	private final int commandCode;

	Command(int commandCode) {
		this.commandCode = commandCode;
	}

	public int getCommandCode() {
		return commandCode;
	}

	public static Optional<Command> findByCommandCode(int code) {
		for (Command command : values()) {
			if (command.getCommandCode() == code) {
				return Optional.of(command);
			}
		}
		return Optional.empty();
	}
}
