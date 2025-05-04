plugins { id("io.vacco.oss.gitflow") version "1.0.1" }

group = "io.vacco.l4zr"
version = "8.37.0"

configure<io.vacco.oss.gitflow.GsPluginProfileExtension> {
  addJ8Spec()
  addClasspathHell()
  sharedLibrary(true, false)
}

val api by configurations

tasks.processResources {
  filesMatching("io/vacco/l4zr/version") {
    expand("projectVersion" to version)
  }
}
