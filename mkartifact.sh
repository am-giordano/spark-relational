username=am-giordano

version=$(grep 'version :=' build.sbt | sed 's/version := "\(.*\)"/\1/')
group_id=$(grep 'organization :=' build.sbt | sed 's/organization := "\(.*\)"/\1/')
artifact_id=$(grep 'name :=' build.sbt | sed 's/name := "\(.*\)"/\1/')
scala_version=$(grep 'scalaVersion :=' build.sbt | sed 's/scalaVersion := "\(.*\)\..*"/\1/')
target_prefix=target/scala-"$scala_version"/"$artifact_id"_"$scala_version"-"$version"
jar="$artifact_id"-"$version".jar
pom="$artifact_id"-"$version".pom

sbt publishLocal

cp "$target_prefix".jar "$jar"
cp "$target_prefix".pom "$pom"

sed -ibackup "s/<groupId>$group_id<\/groupId>/<groupId>$username<\/groupId>/" "$pom"
sed -ibackup "s/<artifactId>${artifact_id}_$scala_version<\/artifactId>/<artifactId>$artifact_id<\/artifactId>/" "$pom"

echo "JAR STRUCTURE"
jar tf "$jar"

echo "POM CONTENTS"
cat "$pom"

zip "releases/$artifact_id-$version.zip" "$jar" "$pom"

rm "$jar" "$pom" "$pom"backup
