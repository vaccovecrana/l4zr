package io.vacco.l4zr;

import io.vacco.l4zr.json.Json;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;
import java.io.InputStreamReader;
import java.util.Objects;

import static j8spec.J8Spec.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class JsonTest {
  static {
    it("Parses JSON", () -> {
      try (var is = JsonTest.class.getResourceAsStream("/example.json")) {
        var lol = Json.parse(new InputStreamReader(Objects.requireNonNull(is)));
        System.out.println("Lol " + lol);
      }
    });
  }
}
