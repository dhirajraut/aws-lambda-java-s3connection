package com.github.dhirajraut.aws;

import com.github.dhirajraut.aws.lambdas.Handler;

public class Runner {
    public static void main(String args[]) {
        Handler handler = new Handler();
        handler.handleRequest(null, null);
    }
}
