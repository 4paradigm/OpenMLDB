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

import io.netty.channel.ChannelHandlerContext;

public interface MysqlServerPacketVisitor {
  void visit(Handshake handshake, ChannelHandlerContext ctx);
  void visit(OkResponse ok, ChannelHandlerContext ctx);
  void visit(EofResponse eof, ChannelHandlerContext ctx);
  void visit(AuthSwitchRequest swi, ChannelHandlerContext ctx);
  void visit(StatisticsResponse stat, ChannelHandlerContext ctx);
  void visit(ErrorResponse error, ChannelHandlerContext ctx);

  void visit(ReplicationEvent repEvent, ChannelHandlerContext ctx);
}
