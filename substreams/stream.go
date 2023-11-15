package substreams

import (
	"context"
	"fmt"
	"io"
	"sync"

	pbursa "github.com/UrsaLab/u-engine/pb"
	sink "github.com/streamingfast/substreams-sink"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

type Stream struct {
	feed chan *pbursa.Block
	err  error

	once   sync.Once
	cancel context.CancelFunc

	sinker *sink.Sinker
}

func newStream(sinker *sink.Sinker, cursor *sink.Cursor) *Stream {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Stream{
		feed:   make(chan *pbursa.Block),
		cancel: cancel,
		sinker: sinker,
	}
	go sinker.Run(ctx, cursor, sinkerHandler(s.feed))
	go func() {
		<-sinker.Terminated()
		if err := sinker.Err(); err != nil {
			s.close(errClosed{err})
		} else {
			s.close(io.EOF)
		}
	}()
	return s
}

func (s *Stream) Recv() (*pbursa.Block, error) {
	if msg, ok := <-s.feed; ok {
		return msg, nil
	}
	return nil, s.err
}

func (s *Stream) Close() error {
	s.close(errClosed{})
	return nil
}

func (s *Stream) close(err error) {
	s.once.Do(func() {
		s.cancel()
		<-s.sinker.Terminated()
		s.err = err
		close(s.feed)
	})
}

type sinkerHandler chan<- *pbursa.Block

func (h sinkerHandler) HandleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h <- &pbursa.Block{Message: &pbursa.Block_Data{Data: &pbursa.BlockData{
		Clock: &pbursa.Clock{
			Id:        data.Clock.Id,
			Number:    data.Clock.Number,
			Timestamp: data.Clock.Timestamp,
		},
		Cursor:  data.Cursor,
		Payload: data.Output.MapOutput,
	}}}:
		return nil
	}
}

func (h sinkerHandler) HandleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h <- &pbursa.Block{Message: &pbursa.Block_Undo{Undo: &pbursa.UndoSignal{
		LastValidBlock: &pbursa.BlockRef{
			Id:     undoSignal.LastValidBlock.Id,
			Number: undoSignal.LastValidBlock.Number,
		},
		LastValidCursor: undoSignal.LastValidCursor,
	}}}:
		return nil
	}
}

type errClosed struct {
	err error
}

func (e errClosed) Error() string {
	if e.err == nil {
		return "substreams closed by user"
	}
	return fmt.Sprintf("substreams closed: %s", e.err)
}

func (e errClosed) Unwrap() error {
	return e.err
}
