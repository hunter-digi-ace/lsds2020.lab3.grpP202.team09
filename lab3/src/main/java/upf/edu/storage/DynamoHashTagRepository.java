package upf.edu.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import twitter4j.Status;
import upf.edu.model.HashTagCount;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    List<String> hashtag =  getHashtags(h.getText());//hacer un for por palabra con hastag
    String lang = h.getLang();
    Long id = h.getId();
    //ejemplo 7 de https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-dynamodb-items.html
    for (int i = 0;  i<hashtag.size();i++){

      UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("hashtag", hashtag.get(i), "lang", lang)
              .withUpdateExpression("set contador = contador + :r,  ids = list_append(ids, :i)")
              //set info.rating = info.rating + :val
              //ADD past_visits :inc, past_chats :inc  SET reset_time = :value
              .withValueMap(new ValueMap().withNumber(":r", 1).withList(":i", Arrays.asList(id)))
              .withReturnValues(ReturnValue.UPDATED_NEW);

      try {
        System.out.println("Updating the item...");
        UpdateItemOutcome outcome = dynamoDBTable.updateItem(updateItemSpec);
        System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

      }
      catch (Exception e) {
          System.err.println("Unable to update item: " + hashtag.get(i));
          Item item = new Item().withPrimaryKey("hashtag", hashtag.get(i), "lang", lang)
                  .withNumber("contador", 1)
                  .withList("ids", Arrays.asList(id));
          try {
            dynamoDBTable.putItem(item);

          }
          catch (Exception j) {
            System.err.println("Unable to create item: " + hashtag.get(i));
            System.err.println(e.getMessage());
          }
      }
    }



  }
  public static List<String> getHashtags(String tweet){

    List<String> hashtags =new ArrayList<String>();
    Pattern pattern = Pattern.compile("#\\p{L}+",Pattern.UNICODE_CHARACTER_CLASS);
    Matcher matcher = pattern.matcher(tweet);
    while (matcher.find())
    {
      hashtags.add(matcher.group().toString());
    }
    return hashtags;
  }

  @Override
  public List<HashTagCount> readTop10(String lang) { // IMPLEMENT ME


    return Collections.emptyList();
  }

}
