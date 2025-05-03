package rqlite.util;

import rqlite.schema.options.ReadConsistencyLevel;
import rqlite.schema.options.UValue;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URLEncoder;
import java.time.Duration;

/* Utility to build a query string from an options object using reflection. */
public class URLUtils {
  public static String makeQueryString(Object options) throws Exception {
    if (options == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    Class<?> clazz = options.getClass();
    for (Field field : clazz.getDeclaredFields()) {
      if (!field.isAnnotationPresent(UValue.class)) continue;
      if (!Modifier.isPublic(field.getModifiers())) {
        field.setAccessible(true);
      }
      UValue annot = field.getAnnotation(UValue.class);
      String key = annot.value();
      Object value = field.get(options);
      if (value == null) continue;
      // Skip default/empty values if requested.
      if (annot.omitEmpty()) {
        if (value instanceof Boolean && !((Boolean) value)) continue;
        if (value instanceof Number && ((Number) value).doubleValue() == 0.0) continue;
        if (value instanceof String && ((String) value).isEmpty()) continue;
        if (value instanceof Duration && ((Duration) value).isZero()) continue;
        if (value instanceof ReadConsistencyLevel && value == ReadConsistencyLevel.UNKNOWN) continue;
      }
      String valueStr = (value instanceof Duration) ? value.toString() : value.toString();
      sb.append(first ? "?" : "&");
      first = false;
      sb.append(URLEncoder.encode(key, "UTF-8"));
      sb.append("=");
      sb.append(URLEncoder.encode(valueStr, "UTF-8"));
    }
    return sb.toString();
  }
}
