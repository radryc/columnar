package columnar

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const (
	logMagic       = "AICOLLG1"
	logKind        = "log_chunk"
	logFormatMajor = 1
)

type LogRecord struct {
	Tenant                 string
	Service                string
	TimestampUnixNano      int64
	SeverityNumber         int64
	SeverityText           string
	Body                   string
	TraceID                string
	SpanID                 string
	ResourceAttributesJSON []byte
	AttributesJSON         []byte
}

type LogChunkHeader struct {
	Version              int          `json:"version"`
	Kind                 string       `json:"kind"`
	RowCount             int          `json:"row_count"`
	MinTimestampUnixNano int64        `json:"min_timestamp_unix_nano"`
	MaxTimestampUnixNano int64        `json:"max_timestamp_unix_nano"`
	Columns              []ColumnMeta `json:"columns"`
}

type LogSearchOptions struct {
	Query        string
	FromUnixNano int64
	ToUnixNano   int64
	Limit        int
}

func EncodeLogChunk(records []LogRecord) ([]byte, LogChunkHeader, error) {
	columns := []struct {
		name     string
		encoding string
		values   func([]LogRecord) ([]byte, error)
	}{
		{name: "tenant", encoding: columnEncodingDictionary, values: func(records []LogRecord) ([]byte, error) {
			return encodeStringDictionary(logStrings(records, func(record LogRecord) string { return record.Tenant }))
		}},
		{name: "service", encoding: columnEncodingDictionary, values: func(records []LogRecord) ([]byte, error) {
			return encodeStringDictionary(logStrings(records, func(record LogRecord) string { return record.Service }))
		}},
		{name: "timestamp_unix_nano", encoding: columnEncodingDeltaOfDelta, values: func(records []LogRecord) ([]byte, error) {
			return encodeDeltaOfDelta(logInt64s(records, func(record LogRecord) int64 { return record.TimestampUnixNano })), nil
		}},
		{name: "severity_number", encoding: columnEncodingDeltaOfDelta, values: func(records []LogRecord) ([]byte, error) {
			return encodeDeltaOfDelta(logInt64s(records, func(record LogRecord) int64 { return record.SeverityNumber })), nil
		}},
		{name: "severity_text", encoding: columnEncodingDictionary, values: func(records []LogRecord) ([]byte, error) {
			return encodeStringDictionary(logStrings(records, func(record LogRecord) string { return record.SeverityText }))
		}},
		{name: "body", encoding: columnEncodingBlob, values: func(records []LogRecord) ([]byte, error) {
			values := make([][]byte, 0, len(records))
			for _, record := range records {
				values = append(values, []byte(record.Body))
			}
			return encodeBlobs(values), nil
		}},
		{name: "trace_id", encoding: columnEncodingDictionary, values: func(records []LogRecord) ([]byte, error) {
			return encodeStringDictionary(logStrings(records, func(record LogRecord) string { return record.TraceID }))
		}},
		{name: "span_id", encoding: columnEncodingDictionary, values: func(records []LogRecord) ([]byte, error) {
			return encodeStringDictionary(logStrings(records, func(record LogRecord) string { return record.SpanID }))
		}},
		{name: "resource_attributes", encoding: columnEncodingDictBlob, values: func(records []LogRecord) ([]byte, error) {
			return encodeBlobDictionary(logBlobs(records, func(record LogRecord) []byte { return record.ResourceAttributesJSON }))
		}},
		{name: "attributes", encoding: columnEncodingDictBlob, values: func(records []LogRecord) ([]byte, error) {
			return encodeBlobDictionary(logBlobs(records, func(record LogRecord) []byte { return record.AttributesJSON }))
		}},
	}

	header := LogChunkHeader{
		Version:  logFormatMajor,
		Kind:     logKind,
		RowCount: len(records),
		Columns:  make([]ColumnMeta, 0, len(columns)),
	}
	if len(records) > 0 {
		header.MinTimestampUnixNano = records[0].TimestampUnixNano
		header.MaxTimestampUnixNano = records[0].TimestampUnixNano
		for _, record := range records[1:] {
			if record.TimestampUnixNano < header.MinTimestampUnixNano {
				header.MinTimestampUnixNano = record.TimestampUnixNano
			}
			if record.TimestampUnixNano > header.MaxTimestampUnixNano {
				header.MaxTimestampUnixNano = record.TimestampUnixNano
			}
		}
	}

	return encodeChunk(logMagic, header, columns, records)
}

func DecodeLogChunk(data []byte) ([]LogRecord, LogChunkHeader, error) {
	return decodeSelectedLogs(data, nil)
}

func SearchLogChunk(data []byte, options LogSearchOptions) ([]LogRecord, LogChunkHeader, error) {
	header, payloadOffset, columns, err := readChunk[LogChunkHeader](data, logMagic)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}

	timestamps, err := decodeDeltaOfDeltaColumn(columns["timestamp_unix_nano"], payloadOffset, data, nil)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	candidates := make([]int, 0, len(timestamps))
	for idx, ts := range timestamps {
		if options.FromUnixNano > 0 && ts < options.FromUnixNano {
			continue
		}
		if options.ToUnixNano > 0 && ts > options.ToUnixNano {
			continue
		}
		candidates = append(candidates, idx)
	}
	if len(candidates) == 0 {
		return nil, header, nil
	}

	query := strings.ToLower(strings.TrimSpace(options.Query))
	selected := candidates
	if query != "" {
		services, err := decodeStringColumn(columns["service"], payloadOffset, data, candidates)
		if err != nil {
			return nil, LogChunkHeader{}, err
		}
		bodies, err := decodeBlobColumn(columns["body"], payloadOffset, data, candidates)
		if err != nil {
			return nil, LogChunkHeader{}, err
		}
		resourceAttrs, err := decodeBlobDictionaryColumn(columns["resource_attributes"], payloadOffset, data, candidates)
		if err != nil {
			return nil, LogChunkHeader{}, err
		}
		attrs, err := decodeBlobDictionaryColumn(columns["attributes"], payloadOffset, data, candidates)
		if err != nil {
			return nil, LogChunkHeader{}, err
		}

		selected = selected[:0]
		for idx, row := range candidates {
			if strings.Contains(strings.ToLower(services[idx]), query) ||
				strings.Contains(strings.ToLower(string(bodies[idx])), query) ||
				strings.Contains(strings.ToLower(string(resourceAttrs[idx])), query) ||
				strings.Contains(strings.ToLower(string(attrs[idx])), query) {
				selected = append(selected, row)
			}
		}
		if len(selected) == 0 {
			return nil, header, nil
		}
	}

	records, _, err := decodeSelectedLogs(data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].TimestampUnixNano == records[j].TimestampUnixNano {
			if records[i].Service == records[j].Service {
				return records[i].Body < records[j].Body
			}
			return records[i].Service < records[j].Service
		}
		return records[i].TimestampUnixNano > records[j].TimestampUnixNano
	})
	if options.Limit > 0 && len(records) > options.Limit {
		records = records[:options.Limit]
	}
	return records, header, nil
}

func decodeSelectedLogs(data []byte, selected []int) ([]LogRecord, LogChunkHeader, error) {
	header, payloadOffset, columns, err := readChunk[LogChunkHeader](data, logMagic)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}

	tenants, err := decodeStringColumn(columns["tenant"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	services, err := decodeStringColumn(columns["service"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	timestamps, err := decodeDeltaOfDeltaColumn(columns["timestamp_unix_nano"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	severityNumbers, err := decodeDeltaOfDeltaColumn(columns["severity_number"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	severityTexts, err := decodeStringColumn(columns["severity_text"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	bodies, err := decodeBlobColumn(columns["body"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	traceIDs, err := decodeStringColumn(columns["trace_id"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	spanIDs, err := decodeStringColumn(columns["span_id"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	resourceAttrs, err := decodeBlobDictionaryColumn(columns["resource_attributes"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}
	attrs, err := decodeBlobDictionaryColumn(columns["attributes"], payloadOffset, data, selected)
	if err != nil {
		return nil, LogChunkHeader{}, err
	}

	out := make([]LogRecord, 0, len(timestamps))
	for idx := range timestamps {
		out = append(out, LogRecord{
			Tenant:                 tenants[idx],
			Service:                services[idx],
			TimestampUnixNano:      timestamps[idx],
			SeverityNumber:         severityNumbers[idx],
			SeverityText:           severityTexts[idx],
			Body:                   string(bodies[idx]),
			TraceID:                traceIDs[idx],
			SpanID:                 spanIDs[idx],
			ResourceAttributesJSON: cloneBytes(resourceAttrs[idx]),
			AttributesJSON:         cloneBytes(attrs[idx]),
		})
	}
	return out, header, nil
}

func readChunk[T any](data []byte, magic string) (T, int, map[string]ColumnMeta, error) {
	var zero T
	if len(data) < len(magic)+4 {
		return zero, 0, nil, fmt.Errorf("chunk too short")
	}
	if string(data[:len(magic)]) != magic {
		return zero, 0, nil, fmt.Errorf("invalid chunk magic")
	}
	headerLen := int(binary.LittleEndian.Uint32(data[len(magic) : len(magic)+4]))
	headerStart := len(magic) + 4
	headerEnd := headerStart + headerLen
	if headerEnd > len(data) {
		return zero, 0, nil, fmt.Errorf("chunk header exceeds payload")
	}
	var header T
	if err := json.Unmarshal(data[headerStart:headerEnd], &header); err != nil {
		return zero, 0, nil, fmt.Errorf("decode header: %w", err)
	}

	var columns []ColumnMeta
	switch typed := any(header).(type) {
	case TraceChunkHeader:
		columns = typed.Columns
	case LogChunkHeader:
		columns = typed.Columns
	case MetricChunkHeader:
		columns = typed.Columns
	default:
		return zero, 0, nil, fmt.Errorf("unsupported chunk header type %T", header)
	}

	columnMap := make(map[string]ColumnMeta, len(columns))
	for _, column := range columns {
		columnMap[column.Name] = column
	}
	return header, headerEnd, columnMap, nil
}

func encodeChunk[T any, R any](magic string, header T, columns []struct {
	name     string
	encoding string
	values   func([]R) ([]byte, error)
}, records []R) ([]byte, T, error) {
	var payload bytes.Buffer
	setColumns := func(metas []ColumnMeta) {
		switch typed := any(&header).(type) {
		case *TraceChunkHeader:
			typed.Columns = metas
		case *LogChunkHeader:
			typed.Columns = metas
		case *MetricChunkHeader:
			typed.Columns = metas
		}
	}

	metas := make([]ColumnMeta, 0, len(columns))
	for _, column := range columns {
		raw, err := column.values(records)
		if err != nil {
			return nil, header, fmt.Errorf("encode %s: %w", column.name, err)
		}
		compressed, err := gzipCompress(raw)
		if err != nil {
			return nil, header, fmt.Errorf("compress %s: %w", column.name, err)
		}
		metas = append(metas, ColumnMeta{
			Name:             column.name,
			Encoding:         column.encoding,
			Compression:      columnCompressionGzip,
			Offset:           payload.Len(),
			Size:             len(compressed),
			UncompressedSize: len(raw),
		})
		if _, err := payload.Write(compressed); err != nil {
			return nil, header, fmt.Errorf("write %s: %w", column.name, err)
		}
	}
	setColumns(metas)

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, header, fmt.Errorf("marshal header: %w", err)
	}

	var out bytes.Buffer
	if _, err := out.WriteString(magic); err != nil {
		return nil, header, err
	}
	var headerLen [4]byte
	binary.LittleEndian.PutUint32(headerLen[:], uint32(len(headerJSON)))
	if _, err := out.Write(headerLen[:]); err != nil {
		return nil, header, err
	}
	if _, err := out.Write(headerJSON); err != nil {
		return nil, header, err
	}
	if _, err := payload.WriteTo(&out); err != nil {
		return nil, header, err
	}
	return out.Bytes(), header, nil
}

func logStrings(records []LogRecord, pick func(LogRecord) string) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, pick(record))
	}
	return out
}

func logInt64s(records []LogRecord, pick func(LogRecord) int64) []int64 {
	out := make([]int64, 0, len(records))
	for _, record := range records {
		out = append(out, pick(record))
	}
	return out
}

func logBlobs(records []LogRecord, pick func(LogRecord) []byte) [][]byte {
	out := make([][]byte, 0, len(records))
	for _, record := range records {
		out = append(out, cloneBytes(pick(record)))
	}
	return out
}
