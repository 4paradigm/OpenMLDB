package cn.paxos.mysql.codec;

public class InitDbCommand extends CommandPacket {

  private final String database;

  public InitDbCommand(int sequenceId, String database) {
    super(sequenceId, Command.COM_INIT_DB);
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }
}
