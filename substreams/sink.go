package substreams

import (
	"fmt"

	"github.com/streamingfast/bstream"
	sink "github.com/streamingfast/substreams-sink"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type Config struct {
	Endpoint string
	APIToken string

	Package          *pbsubstreams.Package
	OutputModuleName string

	Cursor string

	DevelopmentMode bool
	BlockBufferSize int
	FinalBlocksOnly bool
}

func Run(cfg *Config) (*Stream, error) {
	params := defaultSinkParams()

	if cfg.DevelopmentMode {
		params.mode = sink.SubstreamsModeDevelopment
	}
	if cfg.BlockBufferSize > 0 {
		params.opts = append(params.opts, sink.WithBlockDataBuffer(cfg.BlockBufferSize))
	}
	if cfg.FinalBlocksOnly {
		params.opts = append(params.opts, sink.WithFinalBlocksOnly())
	}

	sinker, err := newSinker(cfg, params)
	if err != nil {
		return nil, err
	}

	cursor, err := sink.NewCursor(cfg.Cursor)
	if err != nil {
		return nil, fmt.Errorf("parse cursor: %w", err)
	}

	return newStream(sinker, cursor), nil
}

func ReadManifest(path string, params []string) (*pbsubstreams.Package, error) {
	reader, err := manifest.NewReader(path)
	if err != nil {
		return nil, err
	}

	pkg, err := reader.Read()
	if err != nil {
		return nil, err
	}

	if err = manifest.ApplyParams(params, pkg); err != nil {
		return nil, err
	}

	return pkg, nil
}

func newSinker(cfg *Config, params *sinkParams) (*sink.Sinker, error) {
	graph, err := manifest.NewModuleGraph(cfg.Package.Modules.Modules)
	if err != nil {
		return nil, err
	}
	module, err := graph.Module(cfg.OutputModuleName)
	if err != nil {
		return nil, err
	}
	if module.GetKindMap() == nil {
		return nil, fmt.Errorf("%q is not a `map` module", cfg.OutputModuleName)
	}
	hashes := manifest.NewModuleHashes()
	outputModuleHash, err := hashes.HashModule(cfg.Package.Modules, module, graph)
	if err != nil {
		return nil, fmt.Errorf("hash module %q: %w", module.Name, err)
	}

	params.opts[0] = sink.WithBlockRange(bstream.NewOpenRange(module.InitialBlock))

	clientConfig := client.NewSubstreamsClientConfig(cfg.Endpoint, cfg.APIToken, false, false)

	return sink.New(params.mode, cfg.Package, module, outputModuleHash, clientConfig, params.logger, params.tracer, params.opts...)
}
