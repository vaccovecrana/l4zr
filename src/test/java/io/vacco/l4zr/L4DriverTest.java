package io.vacco.l4zr;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vacco.l4zr.schema.*;
import io.vacco.metolithe.codegen.dao.MtDaoMapper;
import io.vacco.metolithe.codegen.liquibase.*;
import io.vacco.metolithe.core.MtCaseFormat;
import io.vacco.shax.logging.ShOption;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import liquibase.Scope;
import liquibase.command.CommandScope;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.runner.RunWith;
import java.io.*;

import static j8spec.J8Spec.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class L4DriverTest {

  public static final Class<?>[] schema = new Class<?>[] {
    User.class, Device.class, Location.class
  };

  static {
    ShOption.setSysProp(ShOption.IO_VACCO_SHAX_DEVMODE, "true");

    it("Generates schema classes", () -> {
      var root = new MtLb().build(MtCaseFormat.KEEP_CASE, schema);
      var fw = new FileWriter("./src/test/resources/l4-schema.yml");
      new MtLbYaml().writeSchema(root, fw);
    });
    it("Generates schema DAOs", () -> {
      var daoDir = new File("./src/test/java");
      var pkg = "io.vacco.l4zr.dao";
      new MtDaoMapper().mapSchema(daoDir, pkg, MtCaseFormat.KEEP_CASE, schema);
    });
    it("Applies Liquibase changesets",  () -> {
      var hkConfig = new HikariConfig();
      hkConfig.setJdbcUrl("jdbc:rqlite:http://localhost:4001");
      try (var ds = new HikariDataSource(hkConfig)) {
        try (var jdbcConn = new JdbcConnection(ds.getConnection())) {
          var ra = new ClassLoaderResourceAccessor();
          var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(jdbcConn);
          database.setDefaultSchemaName("main");
          Scope.child(Scope.Attr.resourceAccessor, ra, () -> {
            var commandScope = new CommandScope("update");
            commandScope.addArgumentValue("changelogFile", "l4-schema.yml");
            commandScope.addArgumentValue("database", database);
            commandScope.execute();
          });
        }
      }
    });
  }
}
