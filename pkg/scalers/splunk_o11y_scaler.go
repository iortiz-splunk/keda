package scalers

import (
	"context"
	"fmt"
	"math"

	//"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	// "github.com/signalfx/signalfx-go/signalflow/v2"
	"github.com/signalfx/signalflow-client-go/v2/signalflow"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

const (
	queryParam    = "query"
	targetValue   = "targetValue"
	queryAggParam = "queryAggregator"
	actQueryVal   = "activationQueryValue"
	metricName    = "metricName"
	accessToken   = "accessToken"
	realm         = "realm"
)

type splunkO11yScaler struct {
	metricType v2.MetricTargetType
	metadata   *splunkO11yMetadata
	apiClient  *signalflow.Client
	logger     logr.Logger
}

type splunkO11yMetadata struct {
	query                string
	targetValue          float64
	queryAggregator      string
	activationQueryValue float64
	metricName           string
	vType                v2.MetricTargetType
	accessToken          string
	realm                string
}

func NewSplunkO11yScaler(ctx context.Context, config *scalersconfig.ScalerConfig) (Scaler, error) {
	logger := InitializeLogger(config, "splunk-o11y-scaler")

	logger.Info(fmt.Sprintf("Getting MetricType"))
	metricType, err := GetMetricTargetType(config)
	if err != nil {
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}

	logger.Info(fmt.Sprintf("Parsing Metadata"))
	meta, err := ParseMetaData(config, logger)
	if err != nil {
		return nil, fmt.Errorf("error parsing %s metadata: %w", scalerName, err)
	}

	logger.Info(fmt.Sprintf("Creating SignalFLow Client"))
	apiClient, err := signalflow.NewClient(
		signalflow.StreamURLForRealm(realm),
		signalflow.AccessToken(accessToken),
		signalflow.OnError(func(err error) {
			error_msg := fmt.Sprintf("error in SignalFlow client: %v\n", err)
			logger.Info(error_msg)
		}))
	if err != nil {
		return nil, fmt.Errorf("error creating SignalFlow client: %w", err)
	}

	return &splunkO11yScaler{
		metricType: metricType,
		metadata:   meta,
		apiClient:  apiClient,
		logger:     logger,
	}, nil
}

func ParseMetaData(config *scalersconfig.ScalerConfig, logger logr.Logger) (*splunkO11yMetadata, error) {
	meta := splunkO11yMetadata{}
	var err error

	// query
	if val, ok := config.TriggerMetadata[queryParam]; ok && val != "" {
		meta.query = val
	} else {
		return nil, fmt.Errorf("error parsing query param %e", err)
	}
	logger.Info(fmt.Sprintf("Parsed Query Param %s", meta.query))

	// metric name
	if val, ok := config.TriggerMetadata[metricName]; ok && val != "" {
		meta.metricName = val
	} else {
		return nil, fmt.Errorf("error parsing %s param %e", metricName, err)
	}

	// targetValue
	if val, ok := config.TriggerMetadata[targetValue]; ok && val != "" {
		t, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing %s", targetValue)
		}
		meta.targetValue = t
	} else {
		if config.AsMetricSource {
			meta.targetValue = 0
		} else {
			return nil, fmt.Errorf("missing %s value", targetValue)
		}
	}
	logger.Info(fmt.Sprintf("Parsed TargetValue %v", meta.targetValue))

	// activationQueryValue
	meta.activationQueryValue = 0
	if val, ok := config.TriggerMetadata[actQueryVal]; ok {
		activationQueryValue, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("%s parsing error %w", actQueryVal, err)
		}
		meta.activationQueryValue = activationQueryValue
	}

	// queryAggregator
	if val, ok := config.TriggerMetadata[queryAggParam]; ok && val != "" {
		queryAggregator := strings.ToLower(val)
		switch queryAggregator {
		case "max", "min", "avg":
			meta.queryAggregator = queryAggregator
		default:
			return nil, fmt.Errorf("queryAggregator value %s has to be one of 'max', 'min', or 'avg'.", queryAggParam)
		}
	} else {
		return nil, fmt.Errorf("queryAggregator value %s has to be one of 'max', 'min', or 'avg'.", queryAggParam)
	}

	// accessToken
	accessToken, err := GetFromAuthOrMeta(config, accessToken)
	if err != nil {
		return nil, err
	}

	logger.Info(fmt.Sprintf("Parsed accessToken %v", accessToken))
	logger.Info(fmt.Sprintf("Token Length: %v", len(accessToken)))

	// Try to clean token
	accessToken = strings.ReplaceAll(accessToken, "\n", "")
	accessToken = strings.ReplaceAll(accessToken, " ", "")
	accessToken = strings.ReplaceAll(accessToken, "\t", "")

	logger.Info(fmt.Sprintf("CLeaned Token: %v", accessToken))
	logger.Info(fmt.Sprintf("Token Length: %v", len(accessToken)))

	meta.accessToken = accessToken

	// realm
	realm, err := GetFromAuthOrMeta(config, realm)
	if err != nil {
		return nil, err
	}
	meta.realm = realm

	logger.Info(fmt.Sprintf("Parsed realm %v", meta.realm))

	return &meta, nil
}

func logMessage(logger logr.Logger, msg string, value float64) {
	if value != -1 {
		msg = fmt.Sprintf("%s -> %v", msg, value)
	} else {
		msg = fmt.Sprintf("%s", msg)
	}
	logger.Info(msg)
}

func (s *splunkO11yScaler) getQueryResult(ctx context.Context) (float64, error) {
	s.logger.Info("getQueryResult")

	// Why Ten Seconds ??
	// var duration time.Duration = 1000000000 // one second in nano seconds
	var duration time.Duration = 10000000000 // ten seconds in nano seconds

	// Need to add the additional parameters
	comp, err := s.apiClient.Execute(context.Background(), &signalflow.ExecuteRequest{
		Program: s.metadata.query,
	})
	if err != nil {
		return -1, fmt.Errorf("error: could not execute signalflow query: %w", err)
	}

	// Why do we force it to sleep ?
	go func() {
		time.Sleep(duration)
		if err := comp.Stop(context.Background()); err != nil {
			s.logger.Info("Failed to stop computation")
		}
	}()

	//logMessage(s.logger, "Received Splunk Observability metrics", -1)
	s.logger.Info("Received Splunk Observability metrics")

	max := math.Inf(0) // Don't see why it can't be 0 either, min and max can be equal if there is only a single value
	min := math.Inf(0) // Min should likely be 0 as query values can be very small float values.
	valueSum := 0.0
	valueCount := 0

	s.logger.Info("getQueryResult -> Now Iterating")
	for msg := range comp.Data() {
		if len(msg.Payloads) == 0 {
			logMessage(s.logger, "getQueryResult -> No data retreived", -1)
			continue
		}

		for _, pl := range msg.Payloads {
			value, ok := pl.Value().(float64)
			if !ok {
				return -1, fmt.Errorf("error: could not convert Splunk Observability metric value to float64")
			}

			logMessage(s.logger, "Encountering value ", value)
			if value > max {
				max = value
			}
			if value < min {
				min = value
			}
			valueSum += value
			valueCount++

			s.logger.Info(fmt.Sprintf("ValueSum: %v, ValueCount: %v", valueSum, valueCount))
		}
	}

	if valueCount > 1 && s.metadata.queryAggregator == "" {
		return 0, fmt.Errorf("query returned more than 1 series; modify the query to return only 1 series or add a queryAggregator")
	}

	// NaN are being returned when no value comes back from the query as its dividing valueSum/ValueCount 0/0. Need to think of how to handle no values better
	switch s.metadata.queryAggregator {
	case "max":
		logMessage(s.logger, "Returning max value ", max)
		return max, nil
	case "min":
		logMessage(s.logger, "Returning min value ", min)
		return min, nil
	case "avg":
		avg := 0.0
		if valueCount > 0.0 {
			avg = valueSum / float64(valueCount)
		}

		logMessage(s.logger, "Returning avg value ", avg)
		return avg, nil
	default:
		return max, nil
	}
}

func (s *splunkO11yScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {

	s.logger.Info("GetMetricsAndActivity")
	num, err := s.getQueryResult(ctx)
	if err != nil {
		s.logger.Error(err, "error getting metrics from Splunk Observability Cloud.")
		return []external_metrics.ExternalMetricValue{}, false, fmt.Errorf("error getting metrics from Splunk Observability Cloud: %w", err)
	}

	s.logger.Info("GenerateMetricInMili")
	metric := GenerateMetricInMili(metricName, num)

	logMessage(s.logger, "num", num)
	logMessage(s.logger, "s.metadata.activationQueryValue", s.metadata.activationQueryValue)

	return []external_metrics.ExternalMetricValue{metric}, num > s.metadata.activationQueryValue, nil
}

func (s *splunkO11yScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {

	s.logger.Info("GetMetricSpecForScaling")
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: s.metadata.metricName,
		},
		Target: GetMetricTargetMili(s.metricType, s.metadata.targetValue),
	}
	metricSpec := v2.MetricSpec{
		External: externalMetric, Type: externalMetricType,
	}
	return []v2.MetricSpec{metricSpec}
}

func (s *splunkO11yScaler) Close(context.Context) error {
	return nil
}
