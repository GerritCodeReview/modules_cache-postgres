load("//tools/bzl:maven_jar.bzl", "maven_jar")

def external_plugin_deps():
  maven_jar(
    name = "mockito",
    artifact = "org.mockito:mockito-core:2.5.0",
    sha1 = "be28d46a52c7f2563580adeca350145e9ce916f8",
    deps = [
      "@byte_buddy//jar",
      "@objenesis//jar",
    ],
  )

  maven_jar(
    name = "byte_buddy",
    artifact = "net.bytebuddy:byte-buddy:1.5.12",
    sha1 = "b1ba1d15f102b36ed43b826488114678d6d413da",
  )

  maven_jar(
    name = "objenesis",
    artifact = "org.objenesis:objenesis:2.4",
    sha1 = "2916b6c96b50c5b3ec4452ed99401db745aabb27",
  )
