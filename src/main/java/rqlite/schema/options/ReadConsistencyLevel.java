package rqlite.schema.options;

/* Read consistency level. */
public enum ReadConsistencyLevel {
  UNKNOWN, NONE, WEAK, STRONG, LINEARIZABLE, AUTO;

  @Override
  public String toString() {
    switch (this) {
      case NONE:
        return "none";
      case WEAK:
        return "weak";
      case STRONG:
        return "strong";
      case LINEARIZABLE:
        return "linearizable";
      case AUTO:
        return "auto";
      default:
        return "unknown";
    }
  }
}
