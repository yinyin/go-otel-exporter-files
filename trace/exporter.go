package trace

import (
	"context"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/protobuf/proto"

	otelexporterfiles "github.com/yinyin/go-otel-exporter-files"
	"github.com/yinyin/go-otel-exporter-files/trace/internal/tracetransform"
)

const MaxRetainHours = 24 * 365 // 1 year

const (
	DefaultRetainHours   = 8
	DefaultFileSizeLimit = 16 * 1024 * 1024
)

const purgeRangeCount = 2

const timestampFormat = time.RFC3339

var b32Enc = base32.NewEncoding("0123456789abcdefghijklmnopqrstuv").WithPadding(base32.NoPadding)

const outputHourMask = 0xFFFFFF
const outputSnBoundary = 0x7FFFFFFD

type Config struct {
	baseFolderPath string
	retainHours    int32
	fileSizeLimit  int
	marshalOpts    proto.MarshalOptions
}

type FilesTraceExporter struct {
	cfg Config

	lckOutput         sync.Mutex
	outputFolderPath  string
	indexFilePath     string
	outputHour        int32
	outputSn          int32
	outputFileName    string
	outputFp          *os.File
	outputStartAt     time.Time
	outputLastWriteAt time.Time
	currentSize       int
}

func NewFilesTraceExporter(options ...Option) (exporter *FilesTraceExporter, err error) {
	cfg := Config{
		retainHours:   DefaultRetainHours,
		fileSizeLimit: DefaultFileSizeLimit,
	}
	for _, opt := range options {
		cfg = opt.applyExporterOption(cfg)
	}
	if cfg.baseFolderPath == "" {
		err = otelexporterfiles.ErrNeedBaseFolderPath
		return
	}
	exporter = &FilesTraceExporter{
		cfg: cfg,
	}
	return
}

func (x *FilesTraceExporter) writeTimestampFile() (err error) {
	t := time.Now().UTC()
	tSeconds := t.Unix()
	var b0 [8]byte
	binary.LittleEndian.PutUint64(b0[:], uint64(tSeconds))
	content := base64.RawURLEncoding.EncodeToString(b0[:]) + "\n" + strconv.FormatInt(tSeconds, 10) + "\n" + t.Format(timestampFormat) + "\n"
	p := filepath.Join(x.outputFolderPath, "_t")
	if err = os.WriteFile(p, []byte(content), 0644); nil != err {
		err = fmt.Errorf("cannot write timestamp file %q: %w", p, err)
	}
	return
}

func (x *FilesTraceExporter) appendIndexRecord() (err error) {
	// p := filepath.Join(x.outputFolderPath, "_index")
	content := x.outputFileName + "\t" + x.outputStartAt.Format(timestampFormat) + " - " + x.outputLastWriteAt.Format(timestampFormat) + "\n"
	fp, err := os.OpenFile(x.indexFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if nil != err {
		err = fmt.Errorf("cannot open index file %q: %w", x.indexFilePath, err)
		return
	}
	defer fp.Close()
	if _, err = fp.WriteString(content); nil != err {
		err = fmt.Errorf("cannot append content to index file %q: %w", x.indexFilePath, err)
	}
	return
}

func (x *FilesTraceExporter) closeOutputFolder() (err error) {
	if x.outputFolderPath == "" {
		return
	}
	err = x.writeTimestampFile()
	return
}

func (x *FilesTraceExporter) closeOutputFile() (err error) {
	if x.outputFp == nil {
		return
	}
	errS := make([]error, 0, 2)
	if err0 := x.outputFp.Close(); nil != err0 {
		errS = append(errS, err0)
	}
	x.outputFp = nil
	x.currentSize = 0
	if err1 := x.appendIndexRecord(); nil != err1 {
		errS = append(errS, err1)
	}
	x.outputSn = x.outputSn + 1
	if len(errS) > 0 {
		err = fmt.Errorf("caught error on closing output file: %w", errors.Join(errS...))
	}
	return
}

func (x *FilesTraceExporter) makeOutputFolderPath(outputHour int32) string {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(outputHour))
	return filepath.Join(x.cfg.baseFolderPath, b32Enc.EncodeToString(buf[1:]))
}

// purgeRecentExpiredOutputFolders remove most two expired output folders.
func (x *FilesTraceExporter) purgeRecentExpiredOutputFolders() (err error) {
	if x.cfg.retainHours < 1 {
		return
	}
	currentHour := int32((time.Now().Unix() / 3600))
	expiredHourBase := currentHour - x.cfg.retainHours - 1
	var errS []error
	for purgeHourOffset := range purgeRangeCount {
		expiredHour := expiredHourBase - int32(purgeHourOffset)
		if expiredHour < 0 {
			break
		}
		p := x.makeOutputFolderPath(expiredHour & outputHourMask)
		if err0 := os.RemoveAll(p); nil != err0 {
			errS = append(errS, fmt.Errorf("cannot remove expired output folder %q: %w", p, err0))
		}
	}
	if len(errS) > 0 {
		err = fmt.Errorf("cannot purge recent expired output folders: %w", errors.Join(errS...))
	}
	return
}

func makeOutputFileName(outputSn int32) (outputFileName string) {
	if outputSn > 0xFFFF {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], uint32(outputSn))
		outputFileName = b32Enc.EncodeToString(buf[:])
	} else {
		var buf [2]byte
		binary.BigEndian.PutUint16(buf[:], uint16(outputSn))
		outputFileName = b32Enc.EncodeToString(buf[:])
	}
	return
}

// prepareOutputFolder create output folder for given masked outputHour.
//
// The output folder will be create and set the following fields when os.MkdirAll succeed:
// - x.outputFolderPath,
// - x.indexFilePath,
// - x.outputHour,
// - x.outputSn.
func (x *FilesTraceExporter) prepareOutputFolder(outputHour int32) (err error) {
	p := x.makeOutputFolderPath(outputHour)
	if err = os.MkdirAll(p, 0o755); nil != err {
		err = fmt.Errorf("cannot create output folder %q: %w", p, err)
		return
	}
	x.outputFolderPath = p
	x.indexFilePath = filepath.Join(p, "_index")
	x.outputHour = outputHour
	x.outputSn = 0
	return
}

func (x *FilesTraceExporter) prepareOutputFp(recordSize int) (err error) {
	outputHour := int32((time.Now().Unix() / 3600) & outputHourMask)
	if outputHour == x.outputHour {
		if (x.currentSize != 0) && ((x.currentSize + recordSize) <= x.cfg.fileSizeLimit) {
			return
		}
		if err = x.closeOutputFile(); nil != err {
			err = fmt.Errorf("cannot close output file %q: %w", x.outputFileName, err)
			return
		}
		if x.outputFolderPath == "" {
			if err = x.prepareOutputFolder(outputHour); nil != err {
				err = fmt.Errorf("cannot prepare output folder for same hour %d: %w", outputHour, err)
				return
			}
		}
	} else {
		var errS []error
		if err0 := x.closeOutputFile(); nil != err0 {
			errS = append(errS, fmt.Errorf("cannot close output file %q: %w", x.outputFileName, err0))
		}
		if err1 := x.closeOutputFolder(); nil != err1 {
			errS = append(errS, fmt.Errorf("cannot close output folder %q: %w", x.outputFolderPath, err1))
		}
		if len(errS) > 0 {
			err = fmt.Errorf("cannot switch to new hour %d: %w", outputHour, errors.Join(errS...))
			return
		}
		if err = x.prepareOutputFolder(outputHour); nil != err {
			err = fmt.Errorf("cannot prepare output folder for next hour %d: %w", outputHour, err)
			return
		}
		x.purgeRecentExpiredOutputFolders()
	}
	if x.outputSn > outputSnBoundary {
		return
	}
	outputFileName := makeOutputFileName(x.outputSn)
	outputFilePath := filepath.Join(x.outputFolderPath, outputFileName)
	fp, err := os.OpenFile(outputFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if nil != err {
		err = fmt.Errorf("cannot create output file %q: %w", outputFilePath, err)
		return
	}
	x.outputFileName = outputFileName
	x.outputFp = fp
	x.outputStartAt = time.Now().UTC()
	x.outputLastWriteAt = x.outputStartAt
	// x.currentSize = 0	// already set to zero in closeOutputFile
	return
}

func (x *FilesTraceExporter) marshalSpans(spans []sdktrace.ReadOnlySpan) (buf []byte, err error) {
	protoSpans := tracetransform.Spans(spans)
	spanCount := len(protoSpans)
	if spanCount == 0 {
		return
	}
	buf = make([]byte, 4) // TODO: make a better guess
	binary.LittleEndian.PutUint32(buf, uint32(spanCount))
	for pbIdx, pbSpan := range protoSpans {
		pbSizeOffset := len(buf)
		buf = append(buf, 0, 0, 0, 0) // reserve 4 bytes for span size
		if buf, err = x.cfg.marshalOpts.MarshalAppend(buf, pbSpan); nil != err {
			err = fmt.Errorf("cannot marshal %d-th span: %w", pbIdx, err)
			return
		}
		pbSize := len(buf) - pbSizeOffset - 4
		binary.LittleEndian.PutUint32(buf[pbSizeOffset:], uint32(pbSize))
	}
	return
}

func (x *FilesTraceExporter) ExportSpans(
	ctx context.Context,
	spans []sdktrace.ReadOnlySpan) (err error) {
	protoSpans := tracetransform.Spans(spans)
	spanCount := len(protoSpans)
	if spanCount == 0 {
		return
	}
	buf, err := x.marshalSpans(spans)
	if nil != err {
		return
	}
	x.lckOutput.Lock()
	defer x.lckOutput.Unlock()
	if err = x.prepareOutputFp(len(buf)); nil != err {
		return
	}
	if x.outputFp == nil {
		return
	}
	if _, err = x.outputFp.Write(buf); nil != err {
		err = fmt.Errorf("cannot write %d spans to output file %q: %w", spanCount, x.outputFileName, err)
	}
	x.outputLastWriteAt = time.Now().UTC()
	x.currentSize = x.currentSize + len(buf)
	return
}

func (x *FilesTraceExporter) Shutdown(ctx context.Context) (err error) {
	var errS []error
	if err0 := x.closeOutputFile(); nil != err0 {
		errS = append(errS, err0)
	}
	if err1 := x.closeOutputFolder(); nil != err1 {
		errS = append(errS, err1)
	}
	if len(errS) > 0 {
		err = fmt.Errorf("caught failure on shutdown exporter: %w", errors.Join(errS...))
	}
	return
}
