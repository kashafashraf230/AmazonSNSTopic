package SNSservice;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;
public class SNSTopic {

    public static String createSNSTopic(){
        AmazonSNS snsClient = AmazonSNSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566","us-east-2"))
                .build();

        // Create SNS topic
        CreateTopicRequest createTopicRequest = new CreateTopicRequest("MyTopic");
        CreateTopicResult createTopicResult = snsClient.createTopic(createTopicRequest);

        System.out.println("Topic ARN: " + createTopicResult.getTopicArn());
        return createTopicResult.getTopicArn();
    }

    public static void publishMsg(String msg, String snsArn){
        AmazonSNS snsClient = AmazonSNSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566","us-east-2"))
                .build();


        // Publish a message to the SNS topic
        PublishRequest publishRequest = new PublishRequest(snsArn, msg);
        PublishResult publishResult = snsClient.publish(publishRequest);

        System.out.println("Message ID: " + publishResult.getMessageId());

    }
}
