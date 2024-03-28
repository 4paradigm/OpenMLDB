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
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.nio.charset.Charset;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractPacketDecoder extends ByteToMessageDecoder implements Constants {

	private final int maxPacketSize;

	public AbstractPacketDecoder(int maxPacketSize) {
		this.maxPacketSize = maxPacketSize;
	}

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		if (in.isReadable(4)) {
			in.markReaderIndex();
			final int packetSize = in.readUnsignedMediumLE();
			if (packetSize > maxPacketSize) {
				throw new TooLongFrameException("Received a packet of size " + packetSize + " but the maximum packet size is " + maxPacketSize);
			}
			final int sequenceId = in.readByte();
			if (!in.isReadable(packetSize)) {
				in.resetReaderIndex();
				return;
			}
			final ByteBuf packet = in.readSlice(packetSize);

			decodePacket(ctx, sequenceId, packet, out);
		}
	}

	protected abstract void decodePacket(ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out);

	protected OkResponse decodeOkResponse(int sequenceId, ByteBuf packet, Set<CapabilityFlags> capabilities,
										Charset charset) {

		final OkResponse.Builder builder = OkResponse.builder()
				.sequenceId(sequenceId)
				.affectedRows(CodecUtils.readLengthEncodedInteger(packet))
				.lastInsertId(CodecUtils.readLengthEncodedInteger(packet));

		final EnumSet<ServerStatusFlag> statusFlags = CodecUtils.readShortEnumSet(packet, ServerStatusFlag.class);
		if (capabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
			builder
					.addStatusFlags(statusFlags)
					.warnings(packet.readUnsignedShortLE());
		} else if (capabilities.contains(CapabilityFlags.CLIENT_TRANSACTIONS)) {
			builder.addStatusFlags(statusFlags);
		}

		if (capabilities.contains(CapabilityFlags.CLIENT_SESSION_TRACK)) {
			builder.info(CodecUtils.readLengthEncodedString(packet, charset));
			if (statusFlags.contains(ServerStatusFlag.SESSION_STATE_CHANGED)) {
				builder.sessionStateChanges(CodecUtils.readLengthEncodedString(packet, charset));
			}
		} else {
			builder.info(CodecUtils.readFixedLengthString(packet, packet.readableBytes(), charset));
		}
		return builder.build();
	}

	protected EofResponse decodeEofResponse(int sequenceId, ByteBuf packet, Set<CapabilityFlags> capabilities) {
		if (capabilities.contains(CapabilityFlags.CLIENT_PROTOCOL_41)) {
			return new EofResponse(
					sequenceId,
					packet.readUnsignedShortLE(),
					CodecUtils.readShortEnumSet(packet, ServerStatusFlag.class));
		} else {
			return new EofResponse(sequenceId, 0);
		}
	}

	protected ErrorResponse decodeErrorResponse(int sequenceId, ByteBuf packet, Charset charset) {
		final int errorNumber = packet.readUnsignedShortLE();

		final byte[] sqlState;
		sqlState = new byte[SQL_STATE_SIZE];
		packet.readBytes(sqlState);

		final String message = CodecUtils.readFixedLengthString(packet, packet.readableBytes(), charset);

		return new ErrorResponse(sequenceId, errorNumber, sqlState, message);
	}

}