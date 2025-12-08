plugins {
  id("io.vacco.oss.gitflow") version "1.0.1"
  id("maven-publish")
}

group = "io.vacco.l4zr"
version = "8.38.0"

// GitHub Packages credentials (optional for local builds)
val ghUser = providers.gradleProperty("gpr.user")
  .orElse(System.getenv("GITHUB_ACTOR") ?: "")
  .get()
val ghToken = providers.gradleProperty("gpr.key")
  .orElse(System.getenv("GITHUB_TOKEN") ?: "")
  .get()

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
  if (ghUser.isNotBlank() && ghToken.isNotBlank()) {
    repositories {
      maven {
        name = "GitHubPackages"
        url = uri("https://maven.pkg.github.com/felixfong227/l4zr")
        credentials {
          username = ghUser
          password = ghToken
        }
      }
    }
  }
}

// Skip publish tasks when credentials are not provided (local builds)
tasks.withType<PublishToMavenRepository>().configureEach {
  onlyIf {
    if (ghUser.isBlank() || ghToken.isBlank()) {
      logger.lifecycle("Skipping ${name} – GitHub Packages credentials not set.")
      false
    } else {
      true
    }
  }
}
