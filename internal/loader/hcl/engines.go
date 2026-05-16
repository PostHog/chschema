package hcl

import (
	"errors"
	"fmt"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
)

// Engine is the typed, decoded form of an engine block. Each ClickHouse
// engine kind is a distinct concrete type. Implementations are value types.
type Engine interface {
	Kind() string
}

type EngineMergeTree struct{}

func (EngineMergeTree) Kind() string { return "merge_tree" }

type EngineReplicatedMergeTree struct {
	ZooPath     string `hcl:"zoo_path"`
	ReplicaName string `hcl:"replica_name"`
}

func (EngineReplicatedMergeTree) Kind() string { return "replicated_merge_tree" }

type EngineReplacingMergeTree struct {
	VersionColumn *string `hcl:"version_column,optional"`
}

func (EngineReplacingMergeTree) Kind() string { return "replacing_merge_tree" }

type EngineReplicatedReplacingMergeTree struct {
	ZooPath       string  `hcl:"zoo_path"`
	ReplicaName   string  `hcl:"replica_name"`
	VersionColumn *string `hcl:"version_column,optional"`
}

func (EngineReplicatedReplacingMergeTree) Kind() string { return "replicated_replacing_merge_tree" }

type EngineSummingMergeTree struct {
	SumColumns []string `hcl:"sum_columns,optional"`
}

func (EngineSummingMergeTree) Kind() string { return "summing_merge_tree" }

type EngineCollapsingMergeTree struct {
	SignColumn string `hcl:"sign_column"`
}

func (EngineCollapsingMergeTree) Kind() string { return "collapsing_merge_tree" }

type EngineReplicatedCollapsingMergeTree struct {
	ZooPath     string `hcl:"zoo_path"`
	ReplicaName string `hcl:"replica_name"`
	SignColumn  string `hcl:"sign_column"`
}

func (EngineReplicatedCollapsingMergeTree) Kind() string { return "replicated_collapsing_merge_tree" }

type EngineAggregatingMergeTree struct{}

func (EngineAggregatingMergeTree) Kind() string { return "aggregating_merge_tree" }

type EngineReplicatedAggregatingMergeTree struct {
	ZooPath     string `hcl:"zoo_path"`
	ReplicaName string `hcl:"replica_name"`
}

func (EngineReplicatedAggregatingMergeTree) Kind() string { return "replicated_aggregating_merge_tree" }

type EngineDistributed struct {
	ClusterName    string  `hcl:"cluster_name"`
	RemoteDatabase string  `hcl:"remote_database"`
	RemoteTable    string  `hcl:"remote_table"`
	ShardingKey    *string `hcl:"sharding_key,optional"`
}

func (EngineDistributed) Kind() string { return "distributed" }

type EngineLog struct{}

func (EngineLog) Kind() string { return "log" }

type EngineKafka struct {
	// Collection is the named-collection reference. Mutually exclusive
	// with every other field; when set, no inline setting may be set.
	Collection *string `hcl:"collection,optional"`

	// Required when Collection is nil.
	BrokerList *string `hcl:"broker_list,optional"`
	TopicList  *string `hcl:"topic_list,optional"`
	GroupName  *string `hcl:"group_name,optional"`
	Format     *string `hcl:"format,optional"`

	// Optional auth.
	SecurityProtocol *string `hcl:"security_protocol,optional"`
	SaslMechanism    *string `hcl:"sasl_mechanism,optional"`
	SaslUsername     *string `hcl:"sasl_username,optional"`
	SaslPassword     *string `hcl:"sasl_password,optional"`

	// Optional numeric tuning.
	NumConsumers         *int64 `hcl:"num_consumers,optional"`
	MaxBlockSize         *int64 `hcl:"max_block_size,optional"`
	SkipBrokenMessages   *int64 `hcl:"skip_broken_messages,optional"`
	PollTimeoutMs        *int64 `hcl:"poll_timeout_ms,optional"`
	PollMaxBatchSize     *int64 `hcl:"poll_max_batch_size,optional"`
	FlushIntervalMs      *int64 `hcl:"flush_interval_ms,optional"`
	ConsumerRescheduleMs *int64 `hcl:"consumer_reschedule_ms,optional"`
	MaxRowsPerMessage    *int64 `hcl:"max_rows_per_message,optional"`
	CompressionLevel     *int64 `hcl:"compression_level,optional"`

	// Optional booleans (introspected as 0/1, presented as bool in HCL).
	CommitEveryBatch     *bool `hcl:"commit_every_batch,optional"`
	ThreadPerConsumer    *bool `hcl:"thread_per_consumer,optional"`
	CommitOnSelect       *bool `hcl:"commit_on_select,optional"`
	AutodetectClientRack *bool `hcl:"autodetect_client_rack,optional"`

	// Optional strings.
	ClientID         *string `hcl:"client_id,optional"`
	Schema           *string `hcl:"schema,optional"`
	HandleErrorMode  *string `hcl:"handle_error_mode,optional"`
	CompressionCodec *string `hcl:"compression_codec,optional"`

	// Extra is the escape valve for kafka_* settings ClickHouse adds in
	// versions we don't yet model. Keys are passed through verbatim and
	// MUST include the `kafka_` prefix (the typed fields above strip it).
	Extra map[string]string `hcl:"extra,optional"`
}

func (EngineKafka) Kind() string { return "kafka" }

// DecodeEngine dispatches on spec.Kind and decodes the body into a kind-specific
// struct. Returns (nil, nil) when spec is nil.
func DecodeEngine(spec *EngineSpec) (Engine, error) {
	if spec == nil {
		return nil, nil
	}

	var (
		target Engine
		diags  hcl.Diagnostics
	)

	switch spec.Kind {
	case "merge_tree":
		var e EngineMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "replicated_merge_tree":
		var e EngineReplicatedMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "replacing_merge_tree":
		var e EngineReplacingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "replicated_replacing_merge_tree":
		var e EngineReplicatedReplacingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "summing_merge_tree":
		var e EngineSummingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "collapsing_merge_tree":
		var e EngineCollapsingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "replicated_collapsing_merge_tree":
		var e EngineReplicatedCollapsingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "aggregating_merge_tree":
		var e EngineAggregatingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "replicated_aggregating_merge_tree":
		var e EngineReplicatedAggregatingMergeTree
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "distributed":
		var e EngineDistributed
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "log":
		var e EngineLog
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	case "kafka":
		var e EngineKafka
		diags = gohcl.DecodeBody(spec.Body, nil, &e)
		target = e
	default:
		return nil, fmt.Errorf("unknown engine kind %q", spec.Kind)
	}

	if diags.HasErrors() {
		return nil, errors.New(diags.Error())
	}
	return target, nil
}
