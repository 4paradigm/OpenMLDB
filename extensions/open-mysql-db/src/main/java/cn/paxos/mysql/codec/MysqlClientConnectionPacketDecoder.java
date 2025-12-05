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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;

import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

public class MysqlClientConnectionPacketDecoder extends AbstractPacketDecoder implements MysqlClientPacketDecoder {

	// receive AuthSwitchResponse while status is 1, otherwise receive HandshakeResponse
	private int authSwitchStatus = 0;
	// keep HandshakeResponse on init handshake
	private HandshakeResponse handshakeResponse;

	public MysqlClientConnectionPacketDecoder() {
		this(DEFAULT_MAX_PACKET_SIZE);
	}

	public MysqlClientConnectionPacketDecoder(int maxPacketSize) {
		super(maxPacketSize);
	}

	@Override
	protected void decodePacket(ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
		if (authSwitchStatus == 0) {
			final EnumSet<CapabilityFlags> clientCapabilities = CodecUtils.readIntEnumSet(packet, CapabilityFlags.class);

			if (!clientCapabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
				throw new DecoderException("MySQL client protocol 4.1 support required");
			}

			final HandshakeResponse.Builder builder = HandshakeResponse.create();
			builder.addCapabilities(clientCapabilities)
					.maxPacketSize((int) packet.readUnsignedIntLE());
			final MysqlCharacterSet characterSet = MysqlCharacterSet.findById(packet.readByte());
			builder.characterSet(characterSet);
			packet.skipBytes(23);
			if (packet.isReadable()) {
				builder.username(CodecUtils.readNullTerminatedString(packet, characterSet.getCharset()));

				final EnumSet<CapabilityFlags> serverCapabilities = CapabilityFlags.getCapabilitiesAttr(ctx.channel());
				final EnumSet<CapabilityFlags> capabilities = EnumSet.copyOf(clientCapabilities);
				capabilities.retainAll(serverCapabilities);

				final int authResponseLength;
				if (capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA)) {
					authResponseLength = (int) CodecUtils.readLengthEncodedInteger(packet);
				} else if (capabilities.contains(CapabilityFlags.CLIENT_SECURE_CONNECTION)) {
					authResponseLength = packet.readUnsignedByte();
				} else {
					authResponseLength = CodecUtils.findNullTermLen(packet);
				}
				builder.addAuthData(packet, authResponseLength);

				if (capabilities.contains(CapabilityFlags.CLIENT_CONNECT_WITH_DB)) {
					builder.database(CodecUtils.readNullTerminatedString(packet, characterSet.getCharset()));
				}

				if (capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH)) {
					builder.authPluginName(CodecUtils.readNullTerminatedString(packet, StandardCharsets.UTF_8));
				}

				if (capabilities.contains(CapabilityFlags.CLIENT_CONNECT_ATTRS)) {
					final long keyValueLen = CodecUtils.readLengthEncodedInteger(packet);
					for (int i = 0; i < keyValueLen; i++) {
						builder.addAttribute(
								CodecUtils.readLengthEncodedString(packet, StandardCharsets.UTF_8),
								CodecUtils.readLengthEncodedString(packet, StandardCharsets.UTF_8));
					}
				}
			}
			HandshakeResponse response = builder.build();
			this.handshakeResponse = response;
			out.add(response);
		} else {
			// receive AuthSwitchResponse after AuthSwitchRequest send to client
			Objects.requireNonNull(this.handshakeResponse, "handshakeResponse is null");
			this.handshakeResponse.setSequenceId(sequenceId);
			this.handshakeResponse.getAuthPluginData().clear();
			this.handshakeResponse.getAuthPluginData().writeBytes(packet, packet.readableBytes());
			out.add(handshakeResponse);
		}
	}

	public void setAuthSwitchStatus(int status) {
		this.authSwitchStatus = status;
	}

}
