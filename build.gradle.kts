plugins {
  id("io.vacco.oss.gitflow") version "1.0.1"
  id("maven-publish")
}

group = "io.vacco.l4zr"
version = "8.38.0"

// GitHub Packages credentials (optional for local builds)
val ghUser = providers.gradleProperty("gpr.user").orElse("").get()
val ghToken = providers.gradleProperty("gpr.key").orElse("").get()
val ghRepo = providers.gradleProperty("gpr.repo").orElse("").get()

require(ghUser.isNotBlank()) { "Missing gpr.user" }
require(ghToken.isNotBlank()) { "Missing gpr.key" }
require(ghRepo.isNotBlank()) { "Missing gpr.repo" }

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
      url = uri(ghRepo)
      credentials {
        username = ghUser
        password = ghToken
      }
    }
  }
}
