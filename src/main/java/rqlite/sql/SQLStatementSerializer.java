package rqlite.sql;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

/* Custom serializer: if parameters exist, output an array [sql, params…]; otherwise a string. */
public class SQLStatementSerializer extends JsonSerializer<SQLStatement> {
  @Override
  public void serialize(SQLStatement s, JsonGenerator gen, SerializerProvider serializers) throws IOException {
    if (s.namedParams != null && !s.namedParams.isEmpty()) {
      gen.writeStartArray();
      gen.writeString(s.sql);
      gen.writeObject(s.namedParams);
      gen.writeEndArray();
    } else if (s.positionalParams != null && !s.positionalParams.isEmpty()) {
      gen.writeStartArray();
      gen.writeString(s.sql);
      for (Object param : s.positionalParams) {
        gen.writeObject(param);
      }
      gen.writeEndArray();
    } else {
      gen.writeString(s.sql);
    }
  }
}
