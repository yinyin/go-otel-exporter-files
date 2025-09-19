package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"

	trace "github.com/yinyin/go-otel-exporter-files/trace"
)

func parseCommandParam() (otlpEndpoint string, insecureMode bool, targetPaths []string, err error) {
	flag.StringVar(&otlpEndpoint, "endpoint", "[::1]:4317", "target OTLP gRPC endpoint")
	flag.BoolVar(&insecureMode, "insecure", false, "allow clear-text connection")
	flag.Parse()
	if len(otlpEndpoint) == 0 {
		err = errors.New("missing required parameter: endpoint")
		return
	}
	targetPaths = flag.Args()
	if len(targetPaths) == 0 {
		err = errors.New("required target paths")
		return
	}
	return
}

func main() {
	otlpEndpoint, insecureMode, targetPaths, err := parseCommandParam()
	if nil != err {
		log.Fatalf("failed to parse command parameters: %v", err)
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	otlpTraceClientOpts := make([]otlptracegrpc.Option, 0, 2)
	if insecureMode {
		otlpTraceClientOpts = append(otlpTraceClientOpts, otlptracegrpc.WithInsecure())
	}
	otlpTraceClientOpts = append(otlpTraceClientOpts, otlptracegrpc.WithEndpoint(otlpEndpoint))
	client := otlptracegrpc.NewClient(otlpTraceClientOpts...)
	if err = client.Start(ctx); nil != err {
		log.Fatalf("failed to start OTLP trace client: %v", err)
		return
	}
	defer client.Stop(context.Background())
	for _, targetPath := range targetPaths {
		if fInfos, err := os.Lstat(targetPath); nil != err {
			log.Printf("cannot read meta of target path %q: %v", targetPath, err)
		} else if fInfos.IsDir() {
			log.Printf("INFO: import trace folder %q ...", targetPath)
			trace.ImportTraceFolder(ctx, client, targetPath)
		} else if fInfos.Mode().IsRegular() {
			log.Printf("INFO: import trace file %q ...", targetPath)
			trace.ImportTraceFile(ctx, client, targetPath)
		} else {
			log.Printf("WARN: skip unsupported target path %q", targetPath)
		}
	}
	log.Print("INFO: completed.")
}
