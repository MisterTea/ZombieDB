rm -Rf gen-java
find thrift/ -type f | xargs -I repme /Users/jgauci/apache/thrift-0.9.0-dev/compiler/cpp/thrift -gen java repme

