package SQSservice;

import SNSservice.SNSTopic;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.SubscribeRequest;
import com.amazonaws.services.sns.model.SubscribeResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.QueueAttributeName;
import java.util.List;

public class SQS {

    public static void main(String[] args){

        String snsArn = SNSTopic.createSNSTopic();
        System.out.println("SNS Topic Created");
        String sqsUrl = SQS.createQueue();
        System.out.println("SQS Created");
        String sqsArn = SQS.getQueueArn(sqsUrl);
        SQS.subscribeSQSToTopic(sqsArn, snsArn);
        System.out.println("SQS subscribed to SNS Topic");
        SNSTopic.publishMsg("Hello, From SNS", snsArn);
        System.out.println("Msg Published");
        SQS.checkSQSmsg(sqsUrl);
        System.out.println("Msg Checked");
    }

    public static String createQueue(){
        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566","us-east-2"))
                .build();

        // Create SQS queue
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyQueue");
        CreateQueueResult createQueueResult = sqsClient.createQueue(createQueueRequest);

        // Print queue ARN and URL
        System.out.println("Queue ARN: " + createQueueResult.getQueueUrl());
        System.out.println("Queue URL: " + createQueueResult.getQueueUrl());

        return createQueueResult.getQueueUrl();
    }

    public static String getQueueArn(String sqsUrl){

        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566","us-east-2"))
                .build();

        GetQueueAttributesRequest getQueueAttributesRequest = new GetQueueAttributesRequest()
                .withQueueUrl(sqsUrl)
                .withAttributeNames(QueueAttributeName.QueueArn);

        GetQueueAttributesResult result = sqsClient.getQueueAttributes(getQueueAttributesRequest);

        String queueArn = result.getAttributes().get(QueueAttributeName.QueueArn.toString());

        System.out.println("Queue ARN: " + queueArn);
        return queueArn;
    }

    public static void subscribeSQSToTopic(String sqsArn, String snsArn){

        AmazonSNS snsClient = AmazonSNSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566","us-east-2"))
                .build();


        // Subscribe SQS to the SNS topic
        SubscribeRequest subscribeRequest = new SubscribeRequest(snsArn, "sqs", sqsArn);
        SubscribeResult subscribeResult = snsClient.subscribe(subscribeRequest);

        System.out.println("Subscription ARN: " + subscribeResult.getSubscriptionArn());

    }

    public static void checkSQSmsg(String sqsUrl){
        AmazonSQS sqsClient = AmazonSQSClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        "http://localhost:4566","us-east-2"))
                .build();


        // Poll the SQS queue for messages
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsUrl);
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();

        if (messages.isEmpty()) {
            System.out.println("No messages in the queue.");
        } else {
            for (Message message : messages) {
                System.out.println("Received message: " + message.getBody());
            }
        }
    }

}
