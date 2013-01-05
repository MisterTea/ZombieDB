ZombieDB
========

ZombieDB is a database wrapper for people who can't think about database internals because they are too busy making awesome happen.  Here are the key features:

- Several key/value database engines are supported, including Cassandra, Jdbm, Redis, and BerkeleyDB.
- Database schema is completely defined by [Thrift](http://thrift.apache.org/) objects.  You don't need to even think about database schema.
- Primary index (key) is automatically created by looking for an item with the name "id"
- Secondary indexes are automatically created for all primitive items in the thrift object with a tag of 0-15.

Motivation
----------

Most database approaches fall into two categories:

1. Structured data.  Whether you are using MongoDB or MySQL, you are expected to provide some schema definition and also explictly specify secondary indices.
2. Unstructured data.  Then there's redis, jdbm, HBase, etc. which give you a huge hashtable (either on disk or distributed) and let you insert key/value pairs.

The 2nd option is sold as a "don't think, use this!" option, which is very appealing for database zombies like you and I.  The problem is, there is still some thinking that has to be done:

- If you want to store a thrift object, you need to think about (de)serialization.
- If you want to store a thrift object without a unique ID, you need to think.
- If you need to search for a value but you have something other than the key, then you do a lot of thinking. For example, "my users have an ID, so I created an ID:User map, but now I need to be able to get all users with the name 'John Smith'.  I am a zombie without a brain, how can I do this??!"

This is where ZombieDB comes in.  ZombieDB will let you insert, update, find, and remove thrift objects.  ZombieDB will also automatically create any necessary secondary indices.  All you have to do is craft your thrift definition with ZombieDB in mind.  Here is an example thrift object definition with some useful comments:


    struct MyThriftObject {
           1:string id,  // IMPORTANT: You must have a string id object.  This is the primary key, it will be indexed.
           2:i32 someNumber,  // This is a primitive type and the tag # is less than 16, it will be indexed
           3:list<i32> someList, // This is not a primitive, it will not be indexed
           7:double d, // Doubles are a special case.  They are not indexed.
           16:string notIndexed // This has a tag # greater than 15, it will not be indexed
    }

This is all ZombieDB needs to make a database that will store any number of MyThriftObject's.

Example usage
-------

For an example of how ZombieDB works, check out test/com/github/mistertea/zombiedb/DatabaseTestBase.java

Maven
-----

ZombieDB is in the central maven repository.  To use, add this code to your pom.xml

    <dependency>
        <groupId>com.github.mistertea</groupId>
        <artifactId>zombiedb</artifactId>
        <version>LATEST</version>
    </dependency>

Comments/Questions
------------------

If you have any comments/questions about ZombieDB, please contact me:  jgmath2000@gmail.com
