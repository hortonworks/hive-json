Hive JSON Schema Finder
===

This project is a rough prototype that I've written to analyze large
collections of JSON documents and discover their Apache Hive
schema. I've used it to anaylyze the githubarchive.org's log data.

To build the project, use Maven (3.0.x) from http://maven.apache.org/.

Building the jar:

% mvn package

Run the program:

% bin/find-json-schema *.json.gz