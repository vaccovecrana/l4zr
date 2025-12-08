import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication

plugins.apply("maven-publish")

extensions.configure(PublishingExtension::class.java) {
  publications {
    register("gpr", MavenPublication::class.java) {
      from(components.findByName("java") ?: error("Missing java component"))
    }
  }
  repositories {
    maven {
      name = "GitHubPackages"
      url = uri(System.getenv("GPR_REPO") ?: error("Missing GPR_REPO"))
      credentials {
        username = System.getenv("GPR_USER") ?: error("Missing GPR_USER")
        password = System.getenv("GPR_KEY") ?: error("Missing GPR_KEY")
      }
    }
  }
}

