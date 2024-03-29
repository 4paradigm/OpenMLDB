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
import java.util.List;
import java.util.Optional;

public class MysqlClientCommandPacketDecoder extends AbstractPacketDecoder
    implements MysqlClientPacketDecoder {

  private final String database;
  private final String userName;
  private final byte[] scramble411;

  public MysqlClientCommandPacketDecoder(String database, String userName, byte[] scramble411) {
    this(Constants.DEFAULT_MAX_PACKET_SIZE, database, userName, scramble411);
  }

  public MysqlClientCommandPacketDecoder(
      int maxPacketSize, String database, String userName, byte[] scramble411) {
    super(maxPacketSize);

    this.database = database;
    this.userName = userName;
    this.scramble411 = scramble411;
  }

  @Override
  protected void decodePacket(
      ChannelHandlerContext ctx, int sequenceId, ByteBuf packet, List<Object> out) {
    final MysqlCharacterSet clientCharset = MysqlCharacterSet.getClientCharsetAttr(ctx.channel());

    final byte commandCode = packet.readByte();
    final Optional<Command> command = Command.findByCommandCode(commandCode);
    if (!command.isPresent()) {
      throw new DecoderException("Unknown command " + commandCode);
    }
    switch (command.get()) {
      case COM_QUERY:
        out.add(
            new QueryCommand(
                sequenceId,
                CodecUtils.readFixedLengthString(
                    packet, packet.readableBytes(), clientCharset.getCharset()),
                database,
                userName,
                scramble411));
        break;
      case COM_INIT_DB:
        out.add(
            new InitDbCommand(
                sequenceId,
                CodecUtils.readFixedLengthString(
                    packet, packet.readableBytes(), clientCharset.getCharset())));
        break;
      default:
        out.add(new CommandPacket(sequenceId, command.get()));
    }
  }
}
