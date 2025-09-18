package trace

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

func ImportTraceFile(ctx context.Context, client otlptrace.Client, traceFilePath string) (err error) {
	fp, err := os.Open(traceFilePath)
	if nil != err {
		return
	}
	defer fp.Close()
	for {
		var i32buf [4]byte
		if _, err = io.ReadFull(fp, i32buf[:]); nil != err {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("cannot have span count from trace file %q: %w", traceFilePath, err)
			}
			return
		}
		remainSpanCount := int(binary.LittleEndian.Uint32(i32buf[:]))
		traceRecords := make([]*tracepb.ResourceSpans, 0, remainSpanCount)
		for remainSpanCount > 0 {
			if _, err = io.ReadFull(fp, i32buf[:]); nil != err {
				err = fmt.Errorf("cannot have span size from trace file %q: %w", traceFilePath, err)
				return
			}
			spanSize := int(binary.LittleEndian.Uint32(i32buf[:]))
			if spanSize < 0 {
				err = fmt.Errorf("invalid span size from trace file %q: %w", traceFilePath, err)
				return
			} else if spanSize == 0 {
				continue
			}
			remainSpanCount--
			spanBuf := make([]byte, spanSize)
			if _, err = io.ReadFull(fp, spanBuf); nil != err {
				err = fmt.Errorf("cannot read span data from trace file %q: %w", traceFilePath, err)
				return
			}
			traceSpan := &tracepb.ResourceSpans{}
			if err = proto.Unmarshal(spanBuf, traceSpan); nil != err {
				err = fmt.Errorf("cannot unmarshal span data from trace file %q: %w", traceFilePath, err)
				return
			}
			traceRecords = append(traceRecords, traceSpan)
		}
		if traceRecordCount := len(traceRecords); traceRecordCount > 0 {
			if err = client.UploadTraces(ctx, traceRecords); nil != err {
				err = fmt.Errorf("cannot upload %d spans from trace file %q: %w", traceRecordCount, traceFilePath, err)
			}
		}
	}
}

func ImportTraceFolder(ctx context.Context, client otlptrace.Client, folderPath string) (err error) {
	entries, err := os.ReadDir(folderPath)
	if err != nil {
		err = fmt.Errorf("cannot read trace folder %q: %w", folderPath, err)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fileName := entry.Name()
		if (len(fileName) > 1) && ((fileName[0] == '_') || (fileName[0] == '.')) {
			continue
		}
		if err = ImportTraceFile(ctx, client, filepath.Join(folderPath, fileName)); nil != err {
			return
		}
	}
	return
}
