set -e
set -x

mvn clean deploy
mvn release:clean
mvn release:prepare
mvn release:perform
