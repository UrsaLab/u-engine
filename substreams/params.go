package substreams

import (
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	"go.uber.org/zap"
)

type sinkParams struct {
	mode sink.SubstreamsMode

	moduleParams []string

	logger *zap.Logger
	tracer logging.Tracer
	opts   []sink.Option
}

func defaultSinkParams() *sinkParams {
	return &sinkParams{
		mode: sink.SubstreamsModeProduction,

		logger: zap.NewNop(),
		tracer: disabledTracer{},
		opts: []sink.Option{
			nil, // placeholder for default block range
			sink.WithBlockDataBuffer(12),
		},
	}
}

type disabledTracer struct{}

func (_ disabledTracer) Enabled() bool { return false }
