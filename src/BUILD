load("@rules_python//python:defs.bzl", "py_library")
load("@typedb_bazel_distribution//pip:rules.bzl", "assemble_pip", "deploy_pip")
load("@typedb_dependencies//distribution:deployment.bzl", "deployment")

py_library(
    name = "langgraph_checkpoint_typedb",
    srcs = glob(["langgraph_checkpoint_typedb/**/*.py"]),
    data = glob(["langgraph_checkpoint_typedb/**/*.tql"]),
    imports = ["."],
    visibility = ["//visibility:public"],
)

assemble_pip(
    name = "assemble-pip",
    target = ":langgraph_checkpoint_typedb",
    package_name = "langgraph-checkpoint-typedb",
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Intended Audience :: Developers",
        "Topic :: Database :: Front-Ends",
    ],
    url = "https://github.com/typedb/langgraph-checkpoint-typedb",
    author = "Ganeshwara Hananda",
    author_email = "ganesh@typedb.com",
    license = "MIT",
    requirements_file = "//:requirements.txt",
    keywords = ["typedb", "langgraph", "langchain", "checkpoint", "database"],
    description = "LangGraph BaseCheckpointSaver implementation backed by TypeDB 3.x",
    long_description_file = "//:README.md",
    python_requires = ">=3.11,<3.13",
)

deploy_pip(
    name = "deploy-pip",
    target = ":assemble-pip",
    snapshot = deployment["pypi"]["snapshot"],
    release = deployment["pypi"]["release"],
)
