Ref [Maven in 5 Minutes](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html), create a demo project:
```
mvn archetype:generate -DgroupId=com.openmldb.demo -DartifactId=demo -DarchetypeArtifactId=maven-archetype-quickstart -DarchetypeVersion=1.4 -DinteractiveMode=false
```

And use `maven-shade-plugin` to package demo with all dependencies.

So you can run:
```
mvn package
java -cp target/demo-1.0-SNAPSHOT.jar com.openmldb.demo.App
```

If got some errors, check the log in console.

If macOS, add openmldb-native dependency and use macos version.
