package rqlite.schema;

/* Options classes. Their fields are annotated so that URLUtils.makeQueryString
   will convert them into URL query parameters. */
public class BackupOptions {
  @UValue(value = "fmt", omitEmpty = true)
  public String format;
  @UValue(value = "vacuum", omitEmpty = true)
  public boolean vacuum;
  @UValue(value = "compress", omitEmpty = true)
  public boolean compress;
  @UValue(value = "noleader", omitEmpty = true)
  public boolean noLeader;
  @UValue(value = "redirect", omitEmpty = true)
  public boolean redirect;
}
