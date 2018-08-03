package com.example.buggy;

import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.SqlParameter;
import com.microsoft.azure.documentdb.SqlParameterCollection;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import org.apache.commons.lang3.Validate;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

public class CosmosUtils {
    public static void initialize(DocumentClient client, String collection, int ttl, String db) {
        Database database = CosmosUtils.deleteAndCreateDatabase(client, db);
        CosmosUtils.createCollection(client, database, collection, "/partitionKey", ttl);
    }

    public static Database getDatabase(DocumentClient client, String database) {
        SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM root r WHERE r.id = @database");
        query.setParameters(new SqlParameterCollection(
                new SqlParameter("@database", database)
        ));

        List<Database> existing = client.queryDatabases(query, null).getQueryIterable().toList();
        Validate.isTrue(existing.size() <= 1, "There are multiple databases to choose from");

        if (!existing.isEmpty()) {
            return existing.get(0);
        }

        throw new NoSuchElementException();
    }

    public static DocumentCollection getCollection(DocumentClient client, Database database, String collectionName) {
        SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM root r WHERE r.id = @collection");
        query.setParameters(new SqlParameterCollection(
                new SqlParameter("@collection", collectionName)
        ));

        List<DocumentCollection> existing = client.queryCollections(database.getSelfLink(), query, null).getQueryIterable().toList();
        Validate.isTrue(existing.size() <= 1, "There are multiple collections to choose from");

        if (!existing.isEmpty()) {
            return existing.get(0);
        }

        throw new NoSuchElementException();
    }

    public static Database deleteAndCreateDatabase(DocumentClient client, String db) {
        try {
            SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM root r WHERE r.id = @database");
            query.setParameters(new SqlParameterCollection(
                    new SqlParameter("@database", db)
            ));

            List<Database> existing = client.queryDatabases(query, null).getQueryIterable().toList();
            Validate.isTrue(existing.size() <= 1, "There are multiple databases to choose from");

            if (!existing.isEmpty()) {
                return existing.get(0);

            }

            Database database = new Database();
            database.setId(db);
            return client.createDatabase(database, null).getResource();


        } catch (DocumentClientException ex) {
            throw new RuntimeException(ex);
        }
    }


    public static DocumentCollection createCollection(DocumentClient client, Database database, String collectionName, String partitionKey, int ttl) {
        SqlQuerySpec query = new SqlQuerySpec("SELECT * FROM root r WHERE r.id = @collection");
        query.setParameters(new SqlParameterCollection(
                new SqlParameter("@collection", collectionName)
        ));

        List<DocumentCollection> existing = client.queryCollections(database.getSelfLink(), query, null).getQueryIterable().toList();
        Validate.isTrue(existing.size() <= 1, "There are multiple collections to choose from");

        if (!existing.isEmpty()) {
            return existing.get(0);
        } else {
            try {
                PartitionKeyDefinition partition = new PartitionKeyDefinition();
                partition.setPaths(Collections.singletonList(partitionKey));

                RequestOptions options = new RequestOptions();
                options.setOfferThroughput(400);

                DocumentCollection collection = new DocumentCollection();
                collection.setId(collectionName);
                collection.setPartitionKey(partition);
                collection.setDefaultTimeToLive(ttl);


                return client.createCollection(database.getSelfLink(), collection, options).getResource();
            } catch (DocumentClientException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
