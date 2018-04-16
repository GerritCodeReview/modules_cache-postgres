load("//tools/bzl:junit.bzl", "junit_tests")
load(
    "//tools/bzl:plugin.bzl",
    "gerrit_plugin",
    "PLUGIN_DEPS",
    "PLUGIN_TEST_DEPS",
)

gerrit_plugin(
    name = "cache-postgres",
    srcs = glob(["src/main/java/**/*.java"]),
    manifest_entries = [
        "Gerrit-PluginName: cache-postgres",
    ],
    resources = glob(["src/main/resources/**/*"]),
)

junit_tests(
    name = "cache_postgres_tests",
    testonly = 1,
    srcs = glob(["src/test/java/**/*.java"]),
    tags = ["cache-postgres"],
    deps = [
        ":cache-postgres__plugin_test_deps",
    ],
)

java_library(
    name = "cache-postgres__plugin_test_deps",
    testonly = 1,
    visibility = ["//visibility:public"],
    exports = PLUGIN_DEPS + PLUGIN_TEST_DEPS + [
        ":cache-postgres__plugin",
        "@mockito//jar",
    ],
)
