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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

import java.util.*;

public class HandshakeResponse extends DefaultByteBufHolder implements MysqlClientPacket {

	private final Set<CapabilityFlags> capabilityFlags = EnumSet.noneOf(CapabilityFlags.class);
	private final int maxPacketSize;
	private final MysqlCharacterSet characterSet;
	private final String username;
	private final String database;
	private String authPluginName;
	private final Map<String, String> attributes = new HashMap<String, String>();
	private int sequenceId = 1;

	private HandshakeResponse(Builder builder) {
		super(builder.authPluginData);
		this.capabilityFlags.addAll(builder.capabilities);
		this.maxPacketSize = builder.maxPacketSize;
		this.characterSet = builder.characterSet;
		this.username = builder.username;
		this.database = builder.database;
		this.authPluginName = builder.authPluginName;
		this.attributes.putAll(builder.attributes);
	}

	public static Builder create() {
		return new Builder();
	}

	public static HandshakeResponse createSslResponse(Set<CapabilityFlags> capabilities, int maxPacketSize,
                                                      MysqlCharacterSet characterSet) {
		return create()
				.maxPacketSize(maxPacketSize)
				.characterSet(characterSet)
				.addCapabilities(capabilities)
				.addCapabilities(CapabilityFlags.CLIENT_SSL)
				.build();
	}

	public ByteBuf getAuthPluginData() {
		return content();
	}

	public Set<CapabilityFlags> getCapabilityFlags() {
		return EnumSet.copyOf(capabilityFlags);
	}

	public int getMaxPacketSize() {
		return maxPacketSize;
	}

	public MysqlCharacterSet getCharacterSet() {
		return characterSet;
	}

	public String getUsername() {
		return username;
	}

	public String getDatabase() {
		return database;
	}

	public String getAuthPluginName() {
		return authPluginName;
	}

	public Map<String, String> getAttributes() {
		return attributes;
	}

	@Override
	public int getSequenceId() {
		return sequenceId;
	}

	public void setAuthPluginName(String authPluginName) {
		this.authPluginName = authPluginName;
	}

	public void setSequenceId(int sequenceId) {
		this.sequenceId = sequenceId;
	}

	public static class Builder extends AbstractAuthPluginDataBuilder<Builder> {
		private int maxPacketSize = Constants.DEFAULT_MAX_PACKET_SIZE;
		private MysqlCharacterSet characterSet = MysqlCharacterSet.DEFAULT;
		private String username;
		private String database;
		private String authPluginName;
		private Map<String, String> attributes = new HashMap<String, String>();

		public Builder maxPacketSize(int maxPacketSize) {
			this.maxPacketSize = maxPacketSize;
			return this;
		}

		public Builder characterSet(MysqlCharacterSet characterSet) {
			Objects.requireNonNull(characterSet, "characterSet can NOT be null");
			this.characterSet = characterSet;
			return this;
		}

		public Builder username(String username) {
			this.username = username;
			return this;
		}

		public Builder database(String database) {
			addCapabilities(CapabilityFlags.CLIENT_CONNECT_WITH_DB);
			this.database = database;
			return this;
		}

		public Builder authPluginName(String authPluginName) {
			addCapabilities(CapabilityFlags.CLIENT_PLUGIN_AUTH);
			this.authPluginName = authPluginName;
			return this;
		}

		public Builder addAttribute(String key, String value) {
			attributes.put(key, value);
			return this;
		}

		public HandshakeResponse build() {
			return new HandshakeResponse(this);
		}
	}
}
