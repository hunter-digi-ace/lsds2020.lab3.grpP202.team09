package upf.edu.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import org.apache.spark.api.java.JavaRDD;
import twitter4j.Status;
import upf.edu.model.HashTagCount;

import java.io.Serializable;
import java.util.*;

public class DynamoHashTagRepository implements IHashtagRepository, Serializable {




  @Override
  public void write(Status h) { // IMPLEMENT ME



    final  String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final  String region = "us-east-1";
    final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            ).withCredentials(new ProfileCredentialsProvider("upf")) /////////////actualizar vuestros datos de aws con nombre upf, no default
            .build();
    final DynamoDB dynamoDB = new DynamoDB(client);
    final Table dynamoDBTable = dynamoDB.getTable("LSDS2020-TwitterHashtags");

    ///////////////////////////////ejemplo de prueba, esto deberia crear algo en la base de datos
    /*
    String hashtag = h.getText();//hacer un for por palabra con hastag
    String lang = h.getLang();
    Long id = h.getId();
    List<Long> list = new ArrayList<Long>();
    list.add(5L);
    //Item item = new Item().withPrimaryKey("hashtag", hashtag , "counter", 1)
    //                      .withString("language", lang).withList("ids", list);
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("hashtag", new AttributeValue().withS("#hashtag"));
    item.put("counter", new AttributeValue().withN("1"));
    item.put("languaje", new AttributeValue().withS("es"));
    */



    int year = 2015;
    String title = "The Big New Movie";


    try {
      System.out.println("Adding a new item...");
      PutItemOutcome outcome = dynamoDBTable
              .putItem(new Item().withPrimaryKey("hashtag", "hola").withInt("count",0));

      System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());

    }
    catch (Exception e) {
      System.err.println("Unable to add item: " + year + " " + title);
      System.err.println(e.getMessage());
    }




  }

  @Override
  public List<HashTagCount> readTop10(String lang) { // IMPLEMENT ME


    return Collections.emptyList();
  }

}
