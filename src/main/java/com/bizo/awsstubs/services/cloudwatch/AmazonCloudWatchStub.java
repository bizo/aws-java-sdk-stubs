package com.bizo.awsstubs.services.cloudwatch;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.*;

public class AmazonCloudWatchStub implements AmazonCloudWatch {
  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEndpoint(final String arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRegion(final Region arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putMetricAlarm(final PutMetricAlarmRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putMetricData(final PutMetricDataRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListMetricsResult listMetrics(final ListMetricsRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ListMetricsResult listMetrics() {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetMetricStatisticsResult getMetricStatistics(final GetMetricStatisticsRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void disableAlarmActions(final DisableAlarmActionsRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeAlarmsResult describeAlarms(final DescribeAlarmsRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeAlarmsResult describeAlarms() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeAlarmsForMetricResult describeAlarmsForMetric(final DescribeAlarmsForMetricRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeAlarmHistoryResult describeAlarmHistory(final DescribeAlarmHistoryRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DescribeAlarmHistoryResult describeAlarmHistory() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void enableAlarmActions(final EnableAlarmActionsRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteAlarms(final DeleteAlarmsRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAlarmState(final SetAlarmStateRequest arg0) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResponseMetadata getCachedResponseMetadata(final AmazonWebServiceRequest arg0) {
    throw new UnsupportedOperationException();
  }
}