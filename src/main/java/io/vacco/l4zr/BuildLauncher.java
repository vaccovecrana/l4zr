package io.vacco.l4zr;

public class BuildLauncher {
  public static void main(String[] args) {
    System.out.println("Use the Gradle build task to produce build/libs/l4zr-" +
      System.getProperty("project.version", "dev") + ".jar");
  }
}

