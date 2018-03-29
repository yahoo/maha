package com.yahoo.maha.parrequest2;

import org.testng.IRetryAnalyzer;
import org.testng.ITestResult;

public class RetryAnalyzerImpl implements IRetryAnalyzer {
    private int retryCount = 0;
    private int maxRetryCount = 3;
    public boolean retry(ITestResult result) {
        if(retryCount < maxRetryCount)
        {
            retryCount++;
            return true;
        }
        return false;
    }
}
