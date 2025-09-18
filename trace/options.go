package trace

import (
	"google.golang.org/protobuf/proto"
)

type Option interface {
	applyExporterOption(cfg Config) Config
}

type simpleOption struct {
	fn func(cfg Config) Config
}

func newSimpleOption(fn func(cfg Config) Config) Option {
	return &simpleOption{fn: fn}
}

func (o *simpleOption) applyExporterOption(cfg Config) Config {
	return o.fn(cfg)
}

func WithBaseFolderPath(baseFolderPath string) Option {
	return newSimpleOption(func(cfg Config) Config {
		cfg.baseFolderPath = baseFolderPath
		return cfg
	})
}

func WithRetainHours(retainHours int32) Option {
	if retainHours < 0 {
		retainHours = 0
	} else if retainHours > MaxRetainHours {
		retainHours = MaxRetainHours
	}
	return newSimpleOption(func(cfg Config) Config {
		cfg.retainHours = retainHours
		return cfg
	})
}

func WithFileSizeLimit(fileSizeLimit int) Option {
	if fileSizeLimit < 1024 {
		fileSizeLimit = 1024
	}
	return newSimpleOption(func(cfg Config) Config {
		cfg.fileSizeLimit = fileSizeLimit
		return cfg
	})
}

func WithProtoBufMarshalOptions(marshalOpts proto.MarshalOptions) Option {
	return newSimpleOption(func(cfg Config) Config {
		cfg.marshalOpts = marshalOpts
		return cfg
	})
}
