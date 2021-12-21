package com.japharr.sample.mongodb;

import com.mongodb.client.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

public class Connection {
  public static void main(String[] args) {
    String connectionString = System.getProperty("mongodb.uri");
    System.out.println("uri: " + connectionString);
    try (MongoClient mongoClient = MongoClients.create(connectionString)) {
      // list all database
      // List<Document> databases = mongoClient.listDatabases().into(new ArrayList<>());
      // databases.forEach(db -> System.out.println(db.toJson()));

      // connecting to a specific database
      MongoDatabase sampleTrainingDB = mongoClient.getDatabase("settlement");
      MongoCollection<Document> regionCollection = sampleTrainingDB.getCollection("region");
      FindIterable<Document> documents = regionCollection.find();
      documents.forEach(r -> {
        System.out.println(r.toJson());
      });
    }
  }
}
