plugins { id("io.vacco.oss.gitflow") version "1.0.1" }

group = "io.vacco.l4zr"
version = "8.38.0"

configure<io.vacco.oss.gitflow.GsPluginProfileExtension> {
  addJ8Spec()
  addClasspathHell()
  sharedLibrary(true, false)
}

dependencies {
  testImplementation("io.vacco.metolithe:mt-codegen:2.40.0")
  testImplementation("org.liquibase:liquibase-core:4.31.1")
  testImplementation("io.vacco.shax:shax:2.0.16.0.4.3")
  testImplementation("com.zaxxer:HikariCP:6.3.0")
}

tasks.processResources {
  filesMatching("io/vacco/l4zr/version") {
    expand("projectVersion" to version)
  }
}

publishing {
  repositories {
    maven {
      name = "GitHubPackages"
      url = uri("https://maven.pkg.github.com/felixfong227/l4zr")
      credentials {
        username = providers.gradleProperty("gpr.user").orElse(System.getenv("GITHUB_ACTOR")).get()
        password = providers.gradleProperty("gpr.key").orElse(System.getenv("GITHUB_TOKEN")).get()
      }
    }
  }
}
