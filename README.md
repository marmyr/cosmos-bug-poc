#### This is a repository made as a PoC for a bug.

When creating a new document in Cosmos DocumentDB with a partition key exceeding 100 bytes, documentdb throws an exception.  

The size limit on a partition key is 1 KB, not 100b. 

##### Steps to reproduce:

1. Edit _application.properties_ and fill it with the credentials to Cosmos DB
2. Run the unit test _thisWillThrowAnException()_ in _BuggyApplicationTests.java_
3. Cosmos will throw an exception.



com.microsoft.azure.documentdb.DocumentClientException: Message: {"Errors":["PartitionKey extracted from document doesn't match the one specified in the header"]}
