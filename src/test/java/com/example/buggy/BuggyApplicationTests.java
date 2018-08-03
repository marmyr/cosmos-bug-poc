package com.example.buggy;

import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collection;
import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BuggyApplicationTests {
	@Value("${cosmos.host}") String host;
	@Value("${cosmos.key}") String key;
	@Value("${cosmos.collectionName}") String collectionName;
	@Value("${cosmos.db}") String dbName;


	private static class DummyObject {
		public final String partitionKey;
		public final String id;
		public final Integer ttl;

		public DummyObject(String partition, String id, int ttl) {
			this.partitionKey = partition;
			this.id = id;
			this.ttl = ttl;
		}
	}

	// This throws: com.microsoft.azure.documentdb.DocumentClientException: Message: {"Errors":["PartitionKey extracted from document doesn't match the one specified in the header"]}
	@Test
	public void thisWillThrowAnException() {
		String partition = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
		Assert.assertEquals(101, partition.length());
		runTest(partition);
	}

	private void runTest(String partition) {
		DocumentClient client = new DocumentClient(host, key, ConnectionPolicy.GetDefault(), ConsistencyLevel.Session);
		CosmosUtils.initialize(client, collectionName, 30, dbName);


		String id = UUID.randomUUID().toString();

		Database database = CosmosUtils.getDatabase(client, dbName);
		DocumentCollection collection = CosmosUtils.getCollection(client, database, collectionName);

		try {
			DummyObject lock = new DummyObject(partition, id, 30);
			client.createDocument(collection.getSelfLink(), lock, null, true);
		} catch (DocumentClientException ex) {
			throw new RuntimeException(ex);
		}
	}


	@Test
	public void thisWillNotThrowAnException() {
		String partition = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
		Assert.assertEquals(100, partition.length());
		runTest(partition);
	}

}
