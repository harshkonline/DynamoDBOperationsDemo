/**
 * Harshad Kulkarni
 * */

package cmpe226;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeAction;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import com.amazonaws.services.dynamodbv2.model.UpdateTableRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateTableResult;

public class DynamoDBClient {
	private String tableName = "ProductCatalog";
	private AmazonDynamoDBClient client = null;

	public DynamoDBClient() throws IOException {
		AWSCredentials credentials = new PropertiesCredentials(
				DynamoDBClient.class
						.getResourceAsStream("AwsCredentials.properties"));
		client = new AmazonDynamoDBClient(credentials);
	}

	public void createTable() {
		logMessage("Creating table " + tableName);
		ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<AttributeDefinition>();
		attributeDefinitions.add(new AttributeDefinition().withAttributeName(
				"Id").withAttributeType("N"));

		ArrayList<KeySchemaElement> ks = new ArrayList<KeySchemaElement>();
		ks.add(new KeySchemaElement().withAttributeName("Id").withKeyType(
				KeyType.HASH));

		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
				.withReadCapacityUnits(10L).withWriteCapacityUnits(10L);

		CreateTableRequest request = new CreateTableRequest()
				.withTableName(tableName)
				.withAttributeDefinitions(attributeDefinitions)
				.withKeySchema(ks)
				.withProvisionedThroughput(provisionedThroughput);

		CreateTableResult result = client.createTable(request);
		logMessage("Created table "
				+ result.getTableDescription().getTableName());
	}

	public static void logMessage(String msg) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(sdf.format(new Date()) + " ==> " + msg);
	}

	public String getTableStatus() {
		TableDescription tableDescription = client.describeTable(
				new DescribeTableRequest().withTableName(tableName)).getTable();
		return tableDescription.getTableStatus();
	}

	public void describeTable() {
		logMessage("Describing table " + tableName);
		TableDescription tableDescription = client.describeTable(
				new DescribeTableRequest().withTableName(tableName)).getTable();
		String desc = String.format(
				"%s: %s \t ReadCapacityUnits: %d \t WriteCapacityUnits: %d",
				tableDescription.getTableStatus(), tableDescription
						.getTableName(), tableDescription
						.getProvisionedThroughput().getReadCapacityUnits(),
				tableDescription.getProvisionedThroughput()
						.getWriteCapacityUnits());
		logMessage(desc);
	}

	public void updateTable() {
		logMessage("Updating table " + tableName);
		ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
				.withReadCapacityUnits(5L).withWriteCapacityUnits(10L);

		UpdateTableRequest updateTableRequest = new UpdateTableRequest()
				.withTableName(tableName).withProvisionedThroughput(
						provisionedThroughput);

		UpdateTableResult result = client.updateTable(updateTableRequest);
		logMessage("Updated table "
				+ result.getTableDescription().getTableName());
	}

	public void listTables() {
		logMessage("Listing tables");
		// Initial value for the first page of table names.
		String lastEvaluatedTableName = null;
		do {

			ListTablesRequest listTablesRequest = new ListTablesRequest()
					.withLimit(10).withExclusiveStartTableName(
							lastEvaluatedTableName);

			ListTablesResult result = client.listTables(listTablesRequest);
			lastEvaluatedTableName = result.getLastEvaluatedTableName();

			for (String name : result.getTableNames()) {
				logMessage(name);
			}

		} while (lastEvaluatedTableName != null);
	}

	public void deleteTable() {
		logMessage("Deleting table " + tableName);
		DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
				.withTableName(tableName);
		DeleteTableResult result = client.deleteTable(deleteTableRequest);
		logMessage("Deleted table "
				+ result.getTableDescription().getTableName());
	}

	public void putItems() {
		logMessage("Putting items into table " + tableName);
		Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();

		// Add bikes.
		item.put("Id", new AttributeValue().withN("201"));
		// Size, followed by some title.
		item.put("Title", new AttributeValue().withS("18-Bike-201"));
		item.put("Description", new AttributeValue().withS("201 Description"));
		item.put("BicycleType", new AttributeValue().withS("Road"));
		item.put("Brand", new AttributeValue().withS("Mountain A")); // Trek,
																		// Specialized.
		item.put("Price", new AttributeValue().withN("100"));
		item.put("Gender", new AttributeValue().withS("M")); // Men's
		item.put("Color",
				new AttributeValue().withSS(Arrays.asList("Red", "Black")));
		item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

		PutItemRequest itemRequest = new PutItemRequest().withTableName(
				tableName).withItem(item);
		client.putItem(itemRequest);
		item.clear();

		item.put("Id", new AttributeValue().withN("202"));
		item.put("Title", new AttributeValue().withS("21-Bike-202"));
		item.put("Description", new AttributeValue().withS("202 Description"));
		item.put("BicycleType", new AttributeValue().withS("Road"));
		item.put("Brand", new AttributeValue().withS("Brand-Company A"));
		item.put("Price", new AttributeValue().withN("200"));
		item.put("Gender", new AttributeValue().withS("M"));
		item.put("Color",
				new AttributeValue().withSS(Arrays.asList("Green", "Black")));
		item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

		itemRequest = new PutItemRequest().withTableName(tableName).withItem(
				item);
		client.putItem(itemRequest);
		item.clear();

		item.put("Id", new AttributeValue().withN("203"));
		item.put("Title", new AttributeValue().withS("19-Bike-203"));
		item.put("Description", new AttributeValue().withS("203 Description"));
		item.put("BicycleType", new AttributeValue().withS("Road"));
		item.put("Brand", new AttributeValue().withS("Brand-Company B"));
		item.put("Price", new AttributeValue().withN("300"));
		item.put("Gender", new AttributeValue().withS("W")); // Women's
		item.put("Color", new AttributeValue().withSS(Arrays.asList("Red",
				"Green", "Black")));
		item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

		itemRequest = new PutItemRequest().withTableName(tableName).withItem(
				item);
		client.putItem(itemRequest);
		item.clear();

		item.put("Id", new AttributeValue().withN("204"));
		item.put("Title", new AttributeValue().withS("18-Bike-204"));
		item.put("Description", new AttributeValue().withS("204 Description"));
		item.put("BicycleType", new AttributeValue().withS("Mountain"));
		item.put("Brand", new AttributeValue().withS("Brand-Company B"));
		item.put("Price", new AttributeValue().withN("400"));
		item.put("Gender", new AttributeValue().withS("W"));
		item.put("Color", new AttributeValue().withSS(Arrays.asList("Red")));
		item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

		itemRequest = new PutItemRequest().withTableName(tableName).withItem(
				item);
		client.putItem(itemRequest);
		item.clear();

		item.put("Id", new AttributeValue().withN("205"));
		item.put("Title", new AttributeValue().withS("20-Bike-205"));
		item.put("Description", new AttributeValue().withS("205 Description"));
		item.put("BicycleType", new AttributeValue().withS("Hybrid"));
		item.put("Brand", new AttributeValue().withS("Brand-Company C"));
		item.put("Price", new AttributeValue().withN("500"));
		item.put("Gender", new AttributeValue().withS("B")); // Boy's
		item.put("Color",
				new AttributeValue().withSS(Arrays.asList("Red", "Black")));
		item.put("ProductCategory", new AttributeValue().withS("Bicycle"));

		itemRequest = new PutItemRequest().withTableName(tableName).withItem(
				item);
		client.putItem(itemRequest);
	}

	public void updateItem() {
		Map<String, AttributeValueUpdate> updateItems = new HashMap<String, AttributeValueUpdate>();

		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("Id", new AttributeValue().withN("201"));

		// Add two new authors to the list.
		updateItems
				.put("Color",
						new AttributeValueUpdate().withAction(
								AttributeAction.ADD).withValue(
								new AttributeValue().withSS("White", "Gold")));
		ReturnValue returnValues = ReturnValue.ALL_NEW;

		UpdateItemRequest updateItemRequest = new UpdateItemRequest()
				.withTableName(tableName).withKey(key)
				.withAttributeUpdates(updateItems)
				.withReturnValues(returnValues);

		UpdateItemResult result = client.updateItem(updateItemRequest);

		// Check the response.
		logMessage("Printing item after attribute update...");
		printItem(result.getAttributes());
	}

	public void deleteItem() {
		Map<String, ExpectedAttributeValue> expectedValues = new HashMap<String, ExpectedAttributeValue>();
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("Id", new AttributeValue().withN("205"));

		expectedValues.put("Gender", new ExpectedAttributeValue()
				.withValue(new AttributeValue().withS("B")));

		ReturnValue returnValues = ReturnValue.ALL_OLD;

		DeleteItemRequest deleteItemRequest = new DeleteItemRequest()
				.withTableName(tableName).withKey(key)
				.withExpected(expectedValues).withReturnValues(returnValues);

		DeleteItemResult result = client.deleteItem(deleteItemRequest);

		// Check the response.
		logMessage("Printing item that was deleted...");
		printItem(result.getAttributes());
	}

	public void listItems() {
		logMessage("List all items");
		ScanRequest scanRequest = new ScanRequest().withTableName(tableName);

		ScanResult result = client.scan(scanRequest);
		for (Map<String, AttributeValue> item : result.getItems()) {
			printItem(item);
		}
	}

	private void printItem(Map<String, AttributeValue> attributeList) {
		String itemString = new String();
		for (Map.Entry<String, AttributeValue> item : attributeList.entrySet()) {
			if (!itemString.equals(""))
				itemString += ", ";
			String attributeName = item.getKey();
			AttributeValue value = item.getValue();
			itemString += attributeName
					+ ""
					+ (value.getS() == null ? "" : "=\"" + value.getS() + "\"")
					+ (value.getN() == null ? "" : "=\"" + value.getN() + "\"")
					+ (value.getB() == null ? "" : "=\"" + value.getB() + "\"")
					+ (value.getSS() == null ? "" : "=\"" + value.getSS()
							+ "\"")
					+ (value.getNS() == null ? "" : "=\"" + value.getNS()
							+ "\"")
					+ (value.getBS() == null ? "" : "=\"" + value.getBS()
							+ "\" \n");
		}
		logMessage(itemString);
	}

	public static void main(String[] args) {
		try {
			DynamoDBClient dbClient = new DynamoDBClient();

			dbClient.createTable();
			while (!"ACTIVE".equalsIgnoreCase(dbClient.getTableStatus())) {
				logMessage("Waiting for table being created. Sleeping 10 seconds");
				Thread.sleep(10000);
			}
			dbClient.describeTable();

			dbClient.putItems();
			dbClient.listItems();

			dbClient.updateItem();
			dbClient.deleteItem();

			dbClient.updateTable();
			while ("UPDATING".equalsIgnoreCase(dbClient.getTableStatus())) {
				logMessage("Waiting for table being updated. Sleeping 10 seconds");
				Thread.sleep(10000);
			}
			dbClient.describeTable();
			dbClient.listTables();
			dbClient.deleteTable();
			try {
				while ("DELETING".equalsIgnoreCase(dbClient.getTableStatus())) {
					logMessage("Waiting for table being deleted. Sleeping 10 seconds");
					Thread.sleep(10000);
				}
			} catch (ResourceNotFoundException e) {
			}
			dbClient.listTables();

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
