#mongo-queue-java
[![Build Status](https://travis-ci.org/gaillard/mongo-queue-java.png)](https://travis-ci.org/gaillard/mongo-queue-java)

Java message queue using MongoDB as a backend
Adheres to the 1.0.0 [specification](https://github.com/dominionenterprises/mongo-queue-specification).

##Features

 * Message selection and/or count via MongoDB query
 * Distributes across machines via MongoDB
 * Multi language support through the [specification](https://github.com/dominionenterprises/mongo-queue-specification)
 * Message priority
 * Delayed messages
 * Running message timeout and redeliver
 * Atomic acknowledge and send together
 * Easy index creation based only on payload

##Simplest use

```java
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import gaillard.mongo.Queue;
import java.net.UnknownHostException;

public final class Main {

    public static void main(final String[] args) throws UnknownHostException {
        final Queue queue = new Queue(new MongoClient().getDB("testing").getCollection("messages"));
        queue.send(new BasicDBObject());
        final BasicDBObject message = queue.get(new BasicDBObject(), 60);
        queue.ack(message);
    }
}
```

##Jar

To add the library as a jar simply [Build](#project-build) the project and use the `mongo-queue-java-1.0.0.jar` from the created
`target` directory!

##Maven (TODO: Add project to Sonar OSS repo)

To add the library as a local, per-project dependency use [Maven](http://maven.apache.org)! Simply add a dependency on
to your project's `pom.xml` file such as:

```xml
...
<dependency>
    <groupId>gaillard</groupId>
    <artifactId>mongo-queue-java</artifactId>
    <version>1.0.0</version>
</dependency>
...
```

##Documentation

Found in the [source](src/main/java/gaillard/mongo/Queue.java) itself, take a look!

##Contact

Developers may be contacted at:

 * [Pull Requests](https://github.com/gaillard/mongo-queue-java/pulls)
 * [Issues](https://github.com/gaillard/mongo-queue-java/issues)

##Project Build

Install and start [mongodb](http://www.mongodb.org).
With a checkout of the code get [Maven](http://maven.apache.org) in your PATH and run:

```bash
mvn clean install
```
