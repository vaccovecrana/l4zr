package rqlite.schema.options;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/* Annotation to mark fields for URL query parameters.
   The value element defines the parameter name.
   The omitEmpty flag tells the utility to skip default values. */
@Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
@Target(java.lang.annotation.ElementType.FIELD)
public @interface UValue {
  String value();
  boolean omitEmpty() default false;
}
