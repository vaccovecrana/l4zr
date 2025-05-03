package rqlite.sql;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/* Custom deserializer for SQLStatement. */
public class SQLStatementDeserializer extends JsonDeserializer<SQLStatement> {
  @Override
  public SQLStatement deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    JsonToken token = p.currentToken();
    SQLStatement stmt = new SQLStatement();
    if (token == JsonToken.VALUE_STRING) {
      stmt.sql = p.getValueAsString();
      return stmt;
    } else if (token == JsonToken.START_ARRAY) {
      List<Object> list = p.readValueAs(List.class);
      if (!list.isEmpty()) {
        stmt.sql = list.get(0).toString();
        if (list.size() > 1) {
          Object second = list.get(1);
          if (second instanceof Map) {
            stmt.namedParams = (Map<String, Object>) second;
          } else {
            stmt.positionalParams = list.subList(1, list.size());
          }
        }
      }
      return stmt;
    }
    throw new IOException("Unexpected JSON token for SQLStatement: " + token);
  }
}
