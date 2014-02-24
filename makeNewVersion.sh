set -e
set -x

mvn clean deploy -Dgpg.passphrase
mvn release:clean
mvn release:prepare
mvn release:perform
