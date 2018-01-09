package com._4paradigm.rtidb.client.utils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.LoggerFactory;
import org.testng.*;
import org.testng.xml.XmlSuite;

public class TestReport implements IReporter {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(TestListenerAdapter.class);

  /**
   *
   * @param format
   * @param ms
   * @return
   */
  public static String ms2Time(String format, String ms) {
    SimpleDateFormat sdf2date = new SimpleDateFormat(format);
    int len = ms.length();
    if (len < 13) {
      for (int i = 0; i < 13 - len; i++) {
        ms = ms + "0";
      }
    }
    long timelong = Long.parseLong(ms);
    String result = "";
    try {
      result = sdf2date.format(timelong * 1L);
    } catch (Exception e) {
    }
    return result;
  }


  /**
   *
   * @param ms
   * @return
   */
  public static String ms2Time(String ms) {
    return ms2Time("yyyy-MM-dd HH:mm:ss.S", ms);
  }

  public void generateReport(List<XmlSuite> xmlSuites, List<ISuite> suites, String outputDirectory) {
    String roundMark = String.valueOf(System.nanoTime());
    Set<ITestResult> caseResults = new HashSet<ITestResult>(); // 初始化一个集合，用户存放所有case的执行结果
    for (ISuite suite : suites) { // 遍历所有Suite
      Map<String, ISuiteResult> suiteResultsMap = suite.getResults(); // 获取一个suite中所有test的执行结果：Map<testName,suiteResult>
      Collection<ISuiteResult> iSuiteResults = suiteResultsMap.values();
      for (ISuiteResult suiteResult : iSuiteResults) { // 遍历一个suite的所有Test结果
        ITestContext testContext = suiteResult.getTestContext(); // 获取test的信息
        IResultMap passedTests = testContext.getPassedTests(); // 获取所有passed结果
        IResultMap failedTests = testContext.getFailedTests(); // 获取所有failed结果
        IResultMap skippedTests = testContext.getSkippedTests(); // 获取所有skipped结果
        caseResults.addAll(passedTests.getAllResults()); // 收集三种结果的case执行信息
        caseResults.addAll(failedTests.getAllResults());
        caseResults.addAll(skippedTests.getAllResults());
        if (failedTests.size() == 0 && skippedTests.size() == 0) {
          logger.info(outputDirectory);
        }
      }
    }
    StringBuilder sb = new StringBuilder();
    String format = "|%25s |%80s |%25s |%30s |%10s | %10s|";
    sb.append(String.format(format, "CASE NAME", "PARAMS", "TEST CLASS", "START TIME", "RUN TIME", "RESULT"));
    sb.append("\n");
    for (ITestResult iTestResult : caseResults) { // 遍历所有case的Result
      String testClass = iTestResult.getTestClass().getName().replace("com._4paradigm.rtidb.client.functiontest.cases.", ""); // 获取case所在的类
      String testCaseName = iTestResult.getMethod().getMethodName(); // 获取case方法名
      String params = Arrays.asList(iTestResult.getParameters()).toString();
      params = params.replace("[", "").replace("]", ""); // 获取参数，转换格式
      String groups = Arrays.asList(iTestResult.getMethod().getGroups()).toString();
      groups = groups.replace("[", "").replace("]", ""); // 获取分组，转换格式
      int testCasePri = iTestResult.getMethod().getPriority(); // 优先级
      long testStartTimeLong = iTestResult.getStartMillis(); // 开始时间
      long testEndTimeLong = iTestResult.getEndMillis(); // 结束时间
      String testStartTime = ms2Time(testStartTimeLong + ""); // 转换格式
      Long testRunningTimeLong = testEndTimeLong - testStartTimeLong; // 执行时间
      int testRunningTime = testRunningTimeLong.intValue();
      String caseResult = getStatus(iTestResult.getStatus()); // 获取case执行结果
      String exceptionMessage = String.valueOf(iTestResult.getThrowable()).replace(" ", ""); // 获取异常信息
      sb.append(String.format(format, testCaseName, params, testClass, testStartTime, testRunningTime, caseResult));
      sb.append("\n");
    }
    logger.info("\n" + sb.toString());
  }

  private String getStatus(int status) {
    String statusString = null;
    switch (status) {
      case 1:
        statusString = "SUCCESS";
        break;
      case 2:
        statusString = "FAILURE";
        break;
      case 3:
        statusString = "SKIP";
        break;
      default:
        break;
    }
    return statusString;
  }
}