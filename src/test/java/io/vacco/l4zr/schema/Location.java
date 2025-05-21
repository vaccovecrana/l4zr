package io.vacco.l4zr.schema;

import io.vacco.metolithe.annotations.*;

@MtEntity public class Location {

  public int lid;

  @MtFk(Device.class)
  public int did;

  @MtVarchar(32)
  @MtNotNull
  public String geoHash8;

}
