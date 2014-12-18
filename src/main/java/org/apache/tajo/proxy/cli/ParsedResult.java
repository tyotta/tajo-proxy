package org.apache.tajo.proxy.cli;


public class ParsedResult {
  public static enum StatementType {
    META,
    STATEMENT
  }

  private final StatementType type;
  private final String historyStatement;
  private final String statement;

  public ParsedResult(StatementType type, String statement, String historyStatement) {
    this.type = type;
    this.statement = statement;
    this.historyStatement = historyStatement;
  }

  public StatementType getType() {
    return type;
  }

  public String getHistoryStatement() {
    return historyStatement.trim();
  }

  public String getStatement() {
    return statement.trim();
  }

  public String toString() {
    return "(" + type.name() + ") " + historyStatement;
  }
}
