package com.github.dhirajraut.aws.lambdas;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.stream.Collectors;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class Handler implements RequestHandler<Object, String> {

    String BUCKET_NAME = "test-bucket";
    String SOURCE_KEY = "source/data.csv";
    String DESTINATION_KEY = "destination/data.csv";

    @Override
    public String handleRequest(Object input, Context context) {

        printMessage(context, "Reading File");
        S3Client client = S3Client.builder().region(Region.US_EAST_1).build();
        InputStream inputStream = client.getObject(
                GetObjectRequest.builder().bucket(BUCKET_NAME).key(SOURCE_KEY).build(),
                ResponseTransformer.toInputStream());

        printMessage(context, "File Contents");
        printMessage(context,
                new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n")));

        printMessage(context, "Writing File");
        PutObjectRequest objectRequest = PutObjectRequest.builder().bucket(BUCKET_NAME).key(DESTINATION_KEY)
                .serverSideEncryption("AES256").build();
        try {
            client.putObject(objectRequest, RequestBody.fromByteBuffer(getRandomByteBuffer(10_000)));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "output";
    }

    private static ByteBuffer getRandomByteBuffer(int size) throws IOException {
        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return ByteBuffer.wrap(b);
    }

    public void printMessage(Context context, String message) {
        if (context != null && context.getLogger() != null) {
            context.getLogger().log(message);
        } else {
            System.out.println(message);
        }
    }
}
