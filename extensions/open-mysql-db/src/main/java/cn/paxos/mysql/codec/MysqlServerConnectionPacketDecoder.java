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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.CodecException;
import io.netty.util.CharsetUtil;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

public class MysqlServerConnectionPacketDecoder extends AbstractPacketDecoder implements MysqlServerPacketDecoder {

	public MysqlServerConnectionPacketDecoder() {
		this(DEFAULT_MAX_PACKET_SIZE);
	}

	public MysqlServerConnectionPacketDecoder(int maxPacketSize) {
		super(maxPacketSize);
	}

	@Override
	protected void decodePacket(ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
		final Channel channel = ctx.channel();
		final Set<CapabilityFlags> capabilities = CapabilityFlags.getCapabilitiesAttr(channel);
		final Charset serverCharset = MysqlCharacterSet.getServerCharsetAttr(channel).getCharset();

		final int header = packet.readByte() & 0xff;
		switch (header) {
			case RESPONSE_OK:
				out.add(decodeOkResponse(sequenceId, packet, capabilities, serverCharset));
				break;
			case RESPONSE_EOF:
				if (capabilities.contains(CapabilityFlags.CLIENT_PLUGIN_AUTH)) {
					decodeAuthSwitchRequest(sequenceId, packet, out);
				} else {
					out.add(decodeEofResponse(sequenceId, packet, capabilities));
				}
				break;
			case RESPONSE_ERROR:
				out.add(decodeErrorResponse(sequenceId, packet, serverCharset));
				break;
			case 1:
				// TODO Decode auth more data packet: https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthMoreData
				throw new UnsupportedOperationException("Implement auth more data");
			default:
				decodeHandshake(packet, out, header);
		}
	}

	private void decodeAuthSwitchRequest(int sequenceId, ByteBuf packet, List<Object> out) {
		// TODO Implement AuthSwitchRequest decode
		throw new UnsupportedOperationException("Implement decodeAuthSwitchRequest decode.");
	}

	private void decodeHandshake(ByteBuf packet, List<Object> out, int protocolVersion) {
		if (protocolVersion < MINIMUM_SUPPORTED_PROTOCOL_VERSION) {
			throw new CodecException("Unsupported version of MySQL");
		}

		final Handshake.Builder builder = Handshake.builder();
		builder
				.protocolVersion(protocolVersion)
				.serverVersion(CodecUtils.readNullTerminatedString(packet))
				.connectionId(packet.readIntLE())
				.addAuthData(packet, Constants.AUTH_PLUGIN_DATA_PART1_LEN);

		packet.skipBytes(1); // Skip auth plugin data terminator
		builder.addCapabilities(CodecUtils.toEnumSet(CapabilityFlags.class, packet.readUnsignedShortLE()));
		if (packet.isReadable()) {
			builder
					.characterSet(MysqlCharacterSet.findById(packet.readByte()))
					.addServerStatus(CodecUtils.readShortEnumSet(packet, ServerStatusFlag.class))
					.addCapabilities(
							CodecUtils.toEnumSet(CapabilityFlags.class, packet.readUnsignedShortLE() << Short.SIZE));
			if (builder.hasCapability(CapabilityFlags.CLIENT_SECURE_CONNECTION)) {
				final int authDataLen = packet.readByte();

				packet.skipBytes(Constants.HANDSHAKE_RESERVED_BYTES); // Skip reserved bytes
				final int readableBytes =
						Math.max(Constants.AUTH_PLUGIN_DATA_PART2_MIN_LEN,
								authDataLen - Constants.AUTH_PLUGIN_DATA_PART1_LEN);
				builder.addAuthData(packet, readableBytes);
				if (builder.hasCapability(CapabilityFlags.CLIENT_PLUGIN_AUTH) && packet.isReadable()) {
					int len = packet.readableBytes();
					if (packet.getByte(packet.readerIndex() + len - 1) == 0) {
						len--;
					}
					builder.authPluginName(CodecUtils.readFixedLengthString(packet, len, CharsetUtil.UTF_8));
					packet.skipBytes(1);
				}
			}
		}
		final Handshake handshake = builder.build();
		out.add(handshake);
	}
}
