// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package job

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/common/promslog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/cloudwatch"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/clients/tagging"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/config"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/job/maxdimassociator"
	"github.com/prometheus-community/yet-another-cloudwatch-exporter/pkg/model"
)

func Test_getFilteredMetricDatas(t *testing.T) {
	type args struct {
		region                    string
		accountID                 string
		namespace                 string
		customTags                []model.Tag
		tagsOnMetrics             []string
		dimensionRegexps          []model.DimensionsRegexp
		dimensionNameRequirements []string
		resources                 []*model.TaggedResource
		metricsList               []*model.Metric
		m                         *model.MetricConfig
	}
	tests := []struct {
		name               string
		args               args
		wantGetMetricsData []model.CloudwatchData
	}{
		{
			"additional dimension",
			args{
				region:     "us-east-1",
				accountID:  "123123123123",
				namespace:  "efs",
				customTags: nil,
				tagsOnMetrics: []string{
					"Value1",
					"Value2",
				},
				dimensionRegexps: config.SupportedServices.GetService("AWS/EFS").ToModelDimensionsRegexp(),
				resources: []*model.TaggedResource{
					{
						ARN: "arn:aws:elasticfilesystem:us-east-1:123123123123:file-system/fs-abc123",
						Tags: []model.Tag{
							{
								Key:   "Tag",
								Value: "some-Tag",
							},
						},
						Namespace: "efs",
						Region:    "us-east-1",
					},
				},
				metricsList: []*model.Metric{
					{
						MetricName: "StorageBytes",
						Dimensions: []model.Dimension{
							{
								Name:  "FileSystemId",
								Value: "fs-abc123",
							},
							{
								Name:  "StorageClass",
								Value: "Standard",
							},
						},
						Namespace: "AWS/EFS",
					},
				},
				m: &model.MetricConfig{
					Name: "StorageBytes",
					Statistics: []string{
						"Average",
					},
					Period:                 60,
					Length:                 600,
					Delay:                  120,
					NilToZero:              false,
					AddCloudwatchTimestamp: false,
				},
			},
			[]model.CloudwatchData{
				{
					MetricName: "StorageBytes",
					Dimensions: []model.Dimension{
						{
							Name:  "FileSystemId",
							Value: "fs-abc123",
						},
						{
							Name:  "StorageClass",
							Value: "Standard",
						},
					},
					ResourceName: "arn:aws:elasticfilesystem:us-east-1:123123123123:file-system/fs-abc123",
					Namespace:    "efs",
					Tags: []model.Tag{
						{
							Key:   "Value1",
							Value: "",
						},
						{
							Key:   "Value2",
							Value: "",
						},
					},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Period:    60,
						Length:    600,
						Delay:     120,
						Statistic: "Average",
					},
					MetricMigrationParams: model.MetricMigrationParams{
						NilToZero:              false,
						AddCloudwatchTimestamp: false,
					},
				},
			},
		},
		{
			"ec2",
			args{
				region:     "us-east-1",
				accountID:  "123123123123",
				namespace:  "ec2",
				customTags: nil,
				tagsOnMetrics: []string{
					"Value1",
					"Value2",
				},
				dimensionRegexps: config.SupportedServices.GetService("AWS/EC2").ToModelDimensionsRegexp(),
				resources: []*model.TaggedResource{
					{
						ARN: "arn:aws:ec2:us-east-1:123123123123:instance/i-12312312312312312",
						Tags: []model.Tag{
							{
								Key:   "Name",
								Value: "some-Node",
							},
						},
						Namespace: "ec2",
						Region:    "us-east-1",
					},
				},
				metricsList: []*model.Metric{
					{
						MetricName: "CPUUtilization",
						Dimensions: []model.Dimension{
							{
								Name:  "InstanceId",
								Value: "i-12312312312312312",
							},
						},
						Namespace: "AWS/EC2",
					},
				},
				m: &model.MetricConfig{
					Name: "CPUUtilization",
					Statistics: []string{
						"Average",
					},
					Period:                 60,
					Length:                 600,
					Delay:                  120,
					NilToZero:              false,
					AddCloudwatchTimestamp: false,
				},
			},
			[]model.CloudwatchData{
				{
					MetricName:   "CPUUtilization",
					ResourceName: "arn:aws:ec2:us-east-1:123123123123:instance/i-12312312312312312",
					Namespace:    "ec2",
					Dimensions: []model.Dimension{
						{
							Name:  "InstanceId",
							Value: "i-12312312312312312",
						},
					},
					Tags: []model.Tag{
						{
							Key:   "Value1",
							Value: "",
						},
						{
							Key:   "Value2",
							Value: "",
						},
					},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Statistic: "Average",
						Period:    60,
						Length:    600,
						Delay:     120,
					},
					MetricMigrationParams: model.MetricMigrationParams{
						NilToZero:              false,
						AddCloudwatchTimestamp: false,
					},
				},
			},
		},
		{
			"kafka",
			args{
				region:     "us-east-1",
				accountID:  "123123123123",
				namespace:  "kafka",
				customTags: nil,
				tagsOnMetrics: []string{
					"Value1",
					"Value2",
				},
				dimensionRegexps: config.SupportedServices.GetService("AWS/Kafka").ToModelDimensionsRegexp(),
				resources: []*model.TaggedResource{
					{
						ARN: "arn:aws:kafka:us-east-1:123123123123:cluster/demo-cluster-1/12312312-1231-1231-1231-123123123123-12",
						Tags: []model.Tag{
							{
								Key:   "Test",
								Value: "Value",
							},
						},
						Namespace: "kafka",
						Region:    "us-east-1",
					},
				},
				metricsList: []*model.Metric{
					{
						MetricName: "GlobalTopicCount",
						Dimensions: []model.Dimension{
							{
								Name:  "Cluster Name",
								Value: "demo-cluster-1",
							},
						},
						Namespace: "AWS/Kafka",
					},
				},
				m: &model.MetricConfig{
					Name: "GlobalTopicCount",
					Statistics: []string{
						"Average",
					},
					Period:                 60,
					Length:                 600,
					Delay:                  120,
					NilToZero:              false,
					AddCloudwatchTimestamp: false,
				},
			},
			[]model.CloudwatchData{
				{
					MetricName: "GlobalTopicCount",
					Dimensions: []model.Dimension{
						{
							Name:  "Cluster Name",
							Value: "demo-cluster-1",
						},
					},
					ResourceName: "arn:aws:kafka:us-east-1:123123123123:cluster/demo-cluster-1/12312312-1231-1231-1231-123123123123-12",
					Namespace:    "kafka",
					Tags: []model.Tag{
						{
							Key:   "Value1",
							Value: "",
						},
						{
							Key:   "Value2",
							Value: "",
						},
					},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Statistic: "Average",
						Period:    60,
						Length:    600,
						Delay:     120,
					},
					MetricMigrationParams: model.MetricMigrationParams{
						NilToZero:              false,
						AddCloudwatchTimestamp: false,
					},
				},
			},
		},
		{
			"alb",
			args{
				region:                    "us-east-1",
				accountID:                 "123123123123",
				namespace:                 "alb",
				customTags:                nil,
				tagsOnMetrics:             nil,
				dimensionRegexps:          config.SupportedServices.GetService("AWS/ApplicationELB").ToModelDimensionsRegexp(),
				dimensionNameRequirements: []string{"LoadBalancer", "TargetGroup"},
				resources: []*model.TaggedResource{
					{
						ARN: "arn:aws:elasticloadbalancing:us-east-1:123123123123:loadbalancer/app/some-ALB/0123456789012345",
						Tags: []model.Tag{
							{
								Key:   "Name",
								Value: "some-ALB",
							},
						},
						Namespace: "alb",
						Region:    "us-east-1",
					},
				},
				metricsList: []*model.Metric{
					{
						MetricName: "RequestCount",
						Dimensions: []model.Dimension{
							{
								Name:  "LoadBalancer",
								Value: "app/some-ALB/0123456789012345",
							},
							{
								Name:  "TargetGroup",
								Value: "targetgroup/some-ALB/9999666677773333",
							},
							{
								Name:  "AvailabilityZone",
								Value: "us-east-1",
							},
						},
						Namespace: "AWS/ApplicationELB",
					},
					{
						MetricName: "RequestCount",
						Dimensions: []model.Dimension{
							{
								Name:  "LoadBalancer",
								Value: "app/some-ALB/0123456789012345",
							},
							{
								Name:  "TargetGroup",
								Value: "targetgroup/some-ALB/9999666677773333",
							},
						},
						Namespace: "AWS/ApplicationELB",
					},
					{
						MetricName: "RequestCount",
						Dimensions: []model.Dimension{
							{
								Name:  "LoadBalancer",
								Value: "app/some-ALB/0123456789012345",
							},
							{
								Name:  "AvailabilityZone",
								Value: "us-east-1",
							},
						},
						Namespace: "AWS/ApplicationELB",
					},
					{
						MetricName: "RequestCount",
						Dimensions: []model.Dimension{
							{
								Name:  "LoadBalancer",
								Value: "app/some-ALB/0123456789012345",
							},
						},
						Namespace: "AWS/ApplicationELB",
					},
				},
				m: &model.MetricConfig{
					Name: "RequestCount",
					Statistics: []string{
						"Sum",
					},
					Period:                 60,
					Length:                 600,
					Delay:                  120,
					NilToZero:              false,
					AddCloudwatchTimestamp: false,
				},
			},
			[]model.CloudwatchData{
				{
					MetricName: "RequestCount",
					Dimensions: []model.Dimension{
						{
							Name:  "LoadBalancer",
							Value: "app/some-ALB/0123456789012345",
						},
						{
							Name:  "TargetGroup",
							Value: "targetgroup/some-ALB/9999666677773333",
						},
					},
					ResourceName: "arn:aws:elasticloadbalancing:us-east-1:123123123123:loadbalancer/app/some-ALB/0123456789012345",
					Namespace:    "alb",
					Tags:         []model.Tag{},
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Statistic: "Sum",
						Period:    60,
						Length:    600,
						Delay:     120,
					},
					MetricMigrationParams: model.MetricMigrationParams{
						NilToZero:              false,
						AddCloudwatchTimestamp: false,
					},
				},
			},
		},
		{
			"sagemaker: ARN contains uppercase letters",
			args{
				region:           "us-east-1",
				accountID:        "123123123123",
				namespace:        "AWS/SageMaker",
				dimensionRegexps: config.SupportedServices.GetService("AWS/SageMaker").ToModelDimensionsRegexp(),
				resources: []*model.TaggedResource{
					{
						ARN: "arn:aws:sagemaker:us-east-1:123123123123:endpoint/someEndpoint",
						Tags: []model.Tag{
							{
								Key:   "Environment",
								Value: "prod",
							},
						},
						Namespace: "sagemaker",
						Region:    "us-east-1",
					},
				},
				metricsList: []*model.Metric{
					{
						MetricName: "Invocation4XXErrors",
						Dimensions: []model.Dimension{
							{Name: "EndpointName", Value: "someEndpoint"},
							{Name: "VariantName", Value: "AllTraffic"},
						},
						Namespace: "AWS/SageMaker",
					},
				},
				m: &model.MetricConfig{
					Name: "Invocation4XXErrors",
					Statistics: []string{
						"Sum",
					},
					Period: 60,
					Length: 600,
					Delay:  120,
				},
			},
			[]model.CloudwatchData{
				{
					MetricName: "Invocation4XXErrors",
					Dimensions: []model.Dimension{
						{Name: "EndpointName", Value: "someEndpoint"},
						{Name: "VariantName", Value: "AllTraffic"},
					},
					ResourceName: "arn:aws:sagemaker:us-east-1:123123123123:endpoint/someEndpoint",
					Namespace:    "AWS/SageMaker",
					GetMetricDataProcessingParams: &model.GetMetricDataProcessingParams{
						Statistic: "Sum",
						Period:    60,
						Length:    600,
						Delay:     120,
					},
					MetricMigrationParams: model.MetricMigrationParams{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assoc := maxdimassociator.NewAssociator(promslog.NewNopLogger(), tt.args.dimensionRegexps, tt.args.resources)
			discoveryJob := model.DiscoveryJob{
				Namespace:                 tt.args.namespace,
				ExportedTagsOnMetrics:     tt.args.tagsOnMetrics,
				DimensionNameRequirements: tt.args.dimensionNameRequirements,
			}
			metricDatas := getFilteredMetricDatas(promslog.NewNopLogger(), discoveryJob, tt.args.metricsList, tt.args.m, assoc, tt.args.accountID, tt.args.region)
			if len(metricDatas) != len(tt.wantGetMetricsData) {
				t.Errorf("len(getFilteredMetricDatas()) = %v, want %v", len(metricDatas), len(tt.wantGetMetricsData))
			}
			for i, got := range metricDatas {
				want := tt.wantGetMetricsData[i]
				assert.Equal(t, want.MetricName, got.MetricName)
				assert.Equal(t, want.ResourceName, got.ResourceName)
				assert.Equal(t, want.Namespace, got.Namespace)
				assert.ElementsMatch(t, want.Dimensions, got.Dimensions)
				assert.ElementsMatch(t, want.Tags, got.Tags)
				assert.Equal(t, want.MetricMigrationParams, got.MetricMigrationParams)
				assert.Equal(t, want.GetMetricDataProcessingParams.Statistic, got.GetMetricDataProcessingParams.Statistic)
				assert.Equal(t, want.GetMetricDataProcessingParams.Length, got.GetMetricDataProcessingParams.Length)
				assert.Equal(t, want.GetMetricDataProcessingParams.Period, got.GetMetricDataProcessingParams.Period)
				assert.Equal(t, want.GetMetricDataProcessingParams.Delay, got.GetMetricDataProcessingParams.Delay)
				assert.Nil(t, got.GetMetricDataResult)
				assert.Nil(t, got.GetMetricStatisticsResult)
			}
		})
	}
}

func Test_getFilteredMetricDatas_inferArnFromDimensions(t *testing.T) {
	metricConfig := &model.MetricConfig{
		Name:       "CPUUtilization",
		Statistics: []string{"Average"},
		Period:     60,
		Length:     600,
	}

	tests := []struct {
		name       string
		namespace  string
		infer      bool
		dimensions []model.Dimension
		wantARN    string
	}{
		{
			name:       "opted out keeps the global ARN",
			namespace:  "AWS/EC2",
			infer:      false,
			dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-0abc123def456"}},
			wantARN:    "global",
		},
		{
			name:       "opted in infers the ARN for an untagged resource",
			namespace:  "AWS/EC2",
			infer:      true,
			dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-0abc123def456"}},
			wantARN:    "arn:aws:ec2:us-east-1:123456789012:instance/i-0abc123def456",
		},
		{
			name:       "opted in falls back to global when no ARN is derivable",
			namespace:  "AWS/EC2",
			infer:      true,
			dimensions: []model.Dimension{{Name: "AutoScalingGroupName", Value: "my-asg"}},
			wantARN:    "global",
		},
		{
			name:       "opted in handles a dimension name containing a space",
			namespace:  "AWS/DirectoryService",
			infer:      true,
			dimensions: []model.Dimension{{Name: "Directory ID", Value: "d-abc123"}},
			wantARN:    "arn:aws:ds:us-east-1:123456789012:directory/d-abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discoveryJob := model.DiscoveryJob{Namespace: tt.namespace}
			if tt.infer {
				cfg := config.ScrapeConf{
					APIVersion: "v1alpha1",
					Discovery: config.Discovery{
						Jobs: []*config.Job{{
							Type:                           tt.namespace,
							Regions:                        []string{"us-east-1"},
							Roles:                          []config.Role{{}},
							Metrics:                        []*config.Metric{{Name: metricConfig.Name, Statistics: metricConfig.Statistics, Period: 60, Length: 600}},
							InferMissingArnsFromDimensions: true,
						}},
					},
				}
				jobsCfg, err := cfg.Validate(promslog.NewNopLogger())
				require.NoError(t, err)
				require.Len(t, jobsCfg.DiscoveryJobs, 1)
				require.NotNil(t, jobsCfg.DiscoveryJobs[0].InferArnFromDimensions,
					"namespace %q should support ARN inference", tt.namespace)
				discoveryJob = jobsCfg.DiscoveryJobs[0]
			}

			metricsList := []*model.Metric{{
				MetricName: metricConfig.Name,
				Namespace:  tt.namespace,
				Dimensions: tt.dimensions,
			}}

			// nopAssociator stands in for "the Tagging API returned nothing for this metric", which
			// is the only branch that consults InferArnFromDimensions.
			got := getFilteredMetricDatas(promslog.NewNopLogger(), discoveryJob, metricsList, metricConfig, nopAssociator{}, "123456789012", "us-east-1")

			require.Len(t, got, 1)
			require.Equal(t, tt.wantARN, got[0].ResourceName)
			require.Equal(t, metricConfig.Name, got[0].MetricName)
		})
	}
}

// ec2DiscoveryJob builds an AWS/EC2 job through the config layer so it carries the real
// dimension regexps and, when opted in, the real ARN template.
func ec2DiscoveryJob(t *testing.T, infer bool) model.DiscoveryJob {
	t.Helper()
	cfg := config.ScrapeConf{
		APIVersion: "v1alpha1",
		Discovery: config.Discovery{
			ExportedTagsOnMetrics: config.ExportedTagsOnMetrics{"AWS/EC2": {"foo:Bar"}},
			Jobs: []*config.Job{{
				Type:                           "AWS/EC2",
				Regions:                        []string{"us-east-1"},
				Roles:                          []config.Role{{}},
				Metrics:                        []*config.Metric{{Name: "CPUUtilization", Statistics: []string{"Average"}, Period: 60, Length: 600}},
				InferMissingArnsFromDimensions: infer,
			}},
		},
	}
	jobsCfg, err := cfg.Validate(promslog.NewNopLogger())
	require.NoError(t, err)
	require.Len(t, jobsCfg.DiscoveryJobs, 1)
	return jobsCfg.DiscoveryJobs[0]
}

func resourceNames(data []*model.CloudwatchData) []string {
	var names []string
	for _, d := range data {
		names = append(names, d.ResourceName)
	}
	return names
}

func Test_getFilteredMetricDatas_inferArnOnAssociatorSkip(t *testing.T) {
	const (
		region      = "us-east-1"
		accountID   = "123456789012"
		taggedARN   = "arn:aws:ec2:us-east-1:123456789012:instance/i-0aaa111bbb222ccc3"
		inferredARN = "arn:aws:ec2:us-east-1:123456789012:instance/i-0ddd444eee555fff6"
	)
	metricConfig := &model.MetricConfig{Name: "CPUUtilization", Statistics: []string{"Average"}, Period: 60, Length: 600}
	tagged := &model.TaggedResource{
		ARN:       taggedARN,
		Namespace: "AWS/EC2",
		Region:    region,
		Tags:      []model.Tag{{Key: "foo:Bar", Value: "baz"}},
	}
	// Both metrics match the EC2 dimension regexp; only the first matches a discovered resource,
	// so the associator asks to skip the second.
	metricsList := []*model.Metric{
		{MetricName: "CPUUtilization", Namespace: "AWS/EC2", Dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-0aaa111bbb222ccc3"}}},
		{MetricName: "CPUUtilization", Namespace: "AWS/EC2", Dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-0ddd444eee555fff6"}}},
	}

	tests := []struct {
		name     string
		infer    bool
		wantARNs []string
		wantTags [][]model.Tag
	}{
		{
			name:     "opted out drops the metric of the undiscovered instance",
			infer:    false,
			wantARNs: []string{taggedARN},
			wantTags: [][]model.Tag{{{Key: "foo:Bar", Value: "baz"}}},
		},
		{
			name:     "opted in infers the ARN of the undiscovered instance",
			infer:    true,
			wantARNs: []string{taggedARN, inferredARN},
			wantTags: [][]model.Tag{{{Key: "foo:Bar", Value: "baz"}}, {{Key: "foo:Bar", Value: ""}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			discoveryJob := ec2DiscoveryJob(t, tt.infer)
			assoc := maxdimassociator.NewAssociator(promslog.NewNopLogger(), discoveryJob.DimensionsRegexps, []*model.TaggedResource{tagged})

			got := getFilteredMetricDatas(promslog.NewNopLogger(), discoveryJob, metricsList, metricConfig, assoc, accountID, region)

			require.Equal(t, tt.wantARNs, resourceNames(got))
			for i, d := range got {
				require.Equal(t, tt.wantTags[i], d.Tags)
			}
		})
	}
}

type stubTaggingClient struct {
	err error
}

func (s stubTaggingClient) GetResources(context.Context, model.DiscoveryJob, string) ([]*model.TaggedResource, error) {
	return nil, s.err
}

type stubCloudwatchClient struct {
	metrics []*model.Metric
}

func (s stubCloudwatchClient) ListMetrics(_ context.Context, _ string, _ *model.MetricConfig, _ bool, fn func(page []*model.Metric)) error {
	fn(s.metrics)
	return nil
}

func (stubCloudwatchClient) GetMetricData(context.Context, []*model.CloudwatchData, string, time.Time, time.Time) []cloudwatch.MetricDataResult {
	return nil
}

func (stubCloudwatchClient) GetMetricStatistics(context.Context, *slog.Logger, []model.Dimension, string, *model.MetricConfig) []*model.MetricStatisticsResult {
	return nil
}

type passthroughProcessor struct{}

func (passthroughProcessor) Run(_ context.Context, _ string, requests []*model.CloudwatchData) ([]*model.CloudwatchData, error) {
	return requests, nil
}

func Test_runDiscoveryJob_noTaggedResources(t *testing.T) {
	metrics := []*model.Metric{
		{MetricName: "CPUUtilization", Namespace: "AWS/EC2", Dimensions: []model.Dimension{{Name: "InstanceId", Value: "i-0ddd444eee555fff6"}}},
	}

	tests := []struct {
		name     string
		infer    bool
		wantARNs []string
	}{
		{
			name:     "opted out exports nothing",
			infer:    false,
			wantARNs: nil,
		},
		{
			name:     "opted in lists metrics and exports the inferred resources",
			infer:    true,
			wantARNs: []string{"arn:aws:ec2:us-east-1:123456789012:instance/i-0ddd444eee555fff6"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := ec2DiscoveryJob(t, tt.infer)

			resources, data := runDiscoveryJob(
				context.Background(),
				promslog.NewNopLogger(),
				job,
				"123456789012",
				"us-east-1",
				stubTaggingClient{err: tagging.ErrExpectedToFindResources},
				stubCloudwatchClient{metrics: metrics},
				passthroughProcessor{},
				nil,
				model.Role{},
			)

			require.Empty(t, resources)
			require.Equal(t, tt.wantARNs, resourceNames(data))
		})
	}
}
