package columnar

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
)

const (
	traceMagic       = "AICOLTR1"
	traceKind        = "trace_chunk"
	traceFormatMajor = 1
)

const (
	columnEncodingDictionary   = "dictionary"
	columnEncodingDeltaOfDelta = "delta_of_delta"
	columnEncodingBlob         = "blob"
	columnEncodingDictBlob     = "dict_blob"
	columnEncodingBitpack      = "bitpack"
	columnEncodingFloatXOR     = "float_xor"
	columnCompressionGzip      = "gzip"
)

type TraceRecord struct {
	Tenant                 string
	TraceID                string
	SpanID                 string
	ParentSpanID           string
	Name                   string
	Kind                   string
	Service                string
	StartUnixNano          int64
	EndUnixNano            int64
	StatusCode             string
	StatusMessage          string
	ResourceAttributesJSON []byte
	AttributesJSON         []byte
	EventsJSON             []byte
}

type ColumnMeta struct {
	Name             string `json:"name"`
	Encoding         string `json:"encoding"`
	Compression      string `json:"compression"`
	Offset           int    `json:"offset"`
	Size             int    `json:"size"`
	UncompressedSize int    `json:"uncompressed_size"`
}

type TraceChunkHeader struct {
	Version          int          `json:"version"`
	Kind             string       `json:"kind"`
	RowCount         int          `json:"row_count"`
	MinStartUnixNano int64        `json:"min_start_unix_nano"`
	MaxEndUnixNano   int64        `json:"max_end_unix_nano"`
	Columns          []ColumnMeta `json:"columns"`
}

func EncodeTraceChunk(records []TraceRecord) ([]byte, TraceChunkHeader, error) {
	columns := []struct {
		name     string
		encoding string
		values   func([]TraceRecord) ([]byte, error)
	}{
		{name: "tenant", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.Tenant }))
		}},
		{name: "trace_id", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.TraceID }))
		}},
		{name: "span_id", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.SpanID }))
		}},
		{name: "parent_span_id", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.ParentSpanID }))
		}},
		{name: "name", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.Name }))
		}},
		{name: "kind", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.Kind }))
		}},
		{name: "service", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.Service }))
		}},
		{name: "start_unix_nano", encoding: columnEncodingDeltaOfDelta, values: func(records []TraceRecord) ([]byte, error) {
			return encodeDeltaOfDelta(traceInt64s(records, func(record TraceRecord) int64 { return record.StartUnixNano })), nil
		}},
		{name: "end_unix_nano", encoding: columnEncodingDeltaOfDelta, values: func(records []TraceRecord) ([]byte, error) {
			return encodeDeltaOfDelta(traceInt64s(records, func(record TraceRecord) int64 { return record.EndUnixNano })), nil
		}},
		{name: "status_code", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.StatusCode }))
		}},
		{name: "status_message", encoding: columnEncodingDictionary, values: func(records []TraceRecord) ([]byte, error) {
			return encodeStringDictionary(traceStrings(records, func(record TraceRecord) string { return record.StatusMessage }))
		}},
		{name: "resource_attributes", encoding: columnEncodingDictBlob, values: func(records []TraceRecord) ([]byte, error) {
			return encodeBlobDictionary(traceBlobs(records, func(record TraceRecord) []byte { return record.ResourceAttributesJSON }))
		}},
		{name: "attributes", encoding: columnEncodingDictBlob, values: func(records []TraceRecord) ([]byte, error) {
			return encodeBlobDictionary(traceBlobs(records, func(record TraceRecord) []byte { return record.AttributesJSON }))
		}},
		{name: "events", encoding: columnEncodingDictBlob, values: func(records []TraceRecord) ([]byte, error) {
			return encodeBlobDictionary(traceBlobs(records, func(record TraceRecord) []byte { return record.EventsJSON }))
		}},
	}

	header := TraceChunkHeader{
		Version:  traceFormatMajor,
		Kind:     traceKind,
		RowCount: len(records),
		Columns:  make([]ColumnMeta, 0, len(columns)),
	}
	if len(records) > 0 {
		header.MinStartUnixNano = records[0].StartUnixNano
		header.MaxEndUnixNano = records[0].EndUnixNano
		for _, record := range records[1:] {
			if record.StartUnixNano < header.MinStartUnixNano {
				header.MinStartUnixNano = record.StartUnixNano
			}
			if record.EndUnixNano > header.MaxEndUnixNano {
				header.MaxEndUnixNano = record.EndUnixNano
			}
		}
	}

	var payload bytes.Buffer
	for _, column := range columns {
		raw, err := column.values(records)
		if err != nil {
			return nil, TraceChunkHeader{}, fmt.Errorf("encode %s: %w", column.name, err)
		}
		compressed, err := gzipCompress(raw)
		if err != nil {
			return nil, TraceChunkHeader{}, fmt.Errorf("compress %s: %w", column.name, err)
		}
		header.Columns = append(header.Columns, ColumnMeta{
			Name:             column.name,
			Encoding:         column.encoding,
			Compression:      columnCompressionGzip,
			Offset:           payload.Len(),
			Size:             len(compressed),
			UncompressedSize: len(raw),
		})
		if _, err := payload.Write(compressed); err != nil {
			return nil, TraceChunkHeader{}, fmt.Errorf("write %s: %w", column.name, err)
		}
	}

	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, TraceChunkHeader{}, fmt.Errorf("marshal header: %w", err)
	}

	var out bytes.Buffer
	if _, err := out.WriteString(traceMagic); err != nil {
		return nil, TraceChunkHeader{}, err
	}
	var headerLen [4]byte
	binary.LittleEndian.PutUint32(headerLen[:], uint32(len(headerJSON)))
	if _, err := out.Write(headerLen[:]); err != nil {
		return nil, TraceChunkHeader{}, err
	}
	if _, err := out.Write(headerJSON); err != nil {
		return nil, TraceChunkHeader{}, err
	}
	if _, err := payload.WriteTo(&out); err != nil {
		return nil, TraceChunkHeader{}, err
	}
	return out.Bytes(), header, nil
}

func DecodeTraceChunk(data []byte) ([]TraceRecord, TraceChunkHeader, error) {
	header, payloadOffset, columns, err := readTraceChunk(data)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}

	tenants, err := decodeStringColumn(columns["tenant"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	traceIDs, err := decodeStringColumn(columns["trace_id"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	spanIDs, err := decodeStringColumn(columns["span_id"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	parentSpanIDs, err := decodeStringColumn(columns["parent_span_id"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	names, err := decodeStringColumn(columns["name"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	kinds, err := decodeStringColumn(columns["kind"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	services, err := decodeStringColumn(columns["service"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	starts, err := decodeDeltaOfDeltaColumn(columns["start_unix_nano"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	ends, err := decodeDeltaOfDeltaColumn(columns["end_unix_nano"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	statusCodes, err := decodeStringColumn(columns["status_code"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	statusMessages, err := decodeStringColumn(columns["status_message"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	resourceAttrs, err := decodeBlobDictionaryColumn(columns["resource_attributes"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	attrs, err := decodeBlobDictionaryColumn(columns["attributes"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	events, err := decodeBlobDictionaryColumn(columns["events"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}

	out := make([]TraceRecord, 0, header.RowCount)
	for idx := 0; idx < header.RowCount; idx++ {
		out = append(out, TraceRecord{
			Tenant:                 tenants[idx],
			TraceID:                traceIDs[idx],
			SpanID:                 spanIDs[idx],
			ParentSpanID:           parentSpanIDs[idx],
			Name:                   names[idx],
			Kind:                   kinds[idx],
			Service:                services[idx],
			StartUnixNano:          starts[idx],
			EndUnixNano:            ends[idx],
			StatusCode:             statusCodes[idx],
			StatusMessage:          statusMessages[idx],
			ResourceAttributesJSON: cloneBytes(resourceAttrs[idx]),
			AttributesJSON:         cloneBytes(attrs[idx]),
			EventsJSON:             cloneBytes(events[idx]),
		})
	}
	return out, header, nil
}

func SelectTraceByID(data []byte, traceID string) ([]TraceRecord, TraceChunkHeader, error) {
	header, payloadOffset, columns, err := readTraceChunk(data)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	traceIDs, err := decodeStringColumn(columns["trace_id"], payloadOffset, data, nil)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	selected := make([]int, 0, len(traceIDs))
	for idx, candidate := range traceIDs {
		if candidate == traceID {
			selected = append(selected, idx)
		}
	}
	if len(selected) == 0 {
		return nil, header, nil
	}

	tenants, err := decodeStringColumn(columns["tenant"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	spanIDs, err := decodeStringColumn(columns["span_id"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	parentSpanIDs, err := decodeStringColumn(columns["parent_span_id"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	names, err := decodeStringColumn(columns["name"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	kinds, err := decodeStringColumn(columns["kind"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	services, err := decodeStringColumn(columns["service"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	starts, err := decodeDeltaOfDeltaColumn(columns["start_unix_nano"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	ends, err := decodeDeltaOfDeltaColumn(columns["end_unix_nano"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	statusCodes, err := decodeStringColumn(columns["status_code"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	statusMessages, err := decodeStringColumn(columns["status_message"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	resourceAttrs, err := decodeBlobDictionaryColumn(columns["resource_attributes"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	attrs, err := decodeBlobDictionaryColumn(columns["attributes"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}
	events, err := decodeBlobDictionaryColumn(columns["events"], payloadOffset, data, selected)
	if err != nil {
		return nil, TraceChunkHeader{}, err
	}

	out := make([]TraceRecord, 0, len(selected))
	for idx := range selected {
		out = append(out, TraceRecord{
			Tenant:                 tenants[idx],
			TraceID:                traceID,
			SpanID:                 spanIDs[idx],
			ParentSpanID:           parentSpanIDs[idx],
			Name:                   names[idx],
			Kind:                   kinds[idx],
			Service:                services[idx],
			StartUnixNano:          starts[idx],
			EndUnixNano:            ends[idx],
			StatusCode:             statusCodes[idx],
			StatusMessage:          statusMessages[idx],
			ResourceAttributesJSON: cloneBytes(resourceAttrs[idx]),
			AttributesJSON:         cloneBytes(attrs[idx]),
			EventsJSON:             cloneBytes(events[idx]),
		})
	}
	return out, header, nil
}

func readTraceChunk(data []byte) (TraceChunkHeader, int, map[string]ColumnMeta, error) {
	if len(data) < len(traceMagic)+4 {
		return TraceChunkHeader{}, 0, nil, fmt.Errorf("trace chunk too short")
	}
	if string(data[:len(traceMagic)]) != traceMagic {
		return TraceChunkHeader{}, 0, nil, fmt.Errorf("invalid trace chunk magic")
	}
	headerLen := int(binary.LittleEndian.Uint32(data[len(traceMagic) : len(traceMagic)+4]))
	headerStart := len(traceMagic) + 4
	headerEnd := headerStart + headerLen
	if headerEnd > len(data) {
		return TraceChunkHeader{}, 0, nil, fmt.Errorf("trace chunk header exceeds payload")
	}
	var header TraceChunkHeader
	if err := json.Unmarshal(data[headerStart:headerEnd], &header); err != nil {
		return TraceChunkHeader{}, 0, nil, fmt.Errorf("decode header: %w", err)
	}
	columns := make(map[string]ColumnMeta, len(header.Columns))
	for _, column := range header.Columns {
		columns[column.Name] = column
	}
	return header, headerEnd, columns, nil
}

func traceStrings(records []TraceRecord, pick func(TraceRecord) string) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, pick(record))
	}
	return out
}

func traceInt64s(records []TraceRecord, pick func(TraceRecord) int64) []int64 {
	out := make([]int64, 0, len(records))
	for _, record := range records {
		out = append(out, pick(record))
	}
	return out
}

func traceBlobs(records []TraceRecord, pick func(TraceRecord) []byte) [][]byte {
	out := make([][]byte, 0, len(records))
	for _, record := range records {
		out = append(out, cloneBytes(pick(record)))
	}
	return out
}

func encodeStringDictionary(values []string) ([]byte, error) {
	var buf bytes.Buffer
	dictIndex := make(map[string]uint64, len(values))
	dict := make([]string, 0)
	indices := make([]uint64, 0, len(values))
	for _, value := range values {
		index, ok := dictIndex[value]
		if !ok {
			index = uint64(len(dict))
			dictIndex[value] = index
			dict = append(dict, value)
		}
		indices = append(indices, index)
	}
	if err := writeUvarint(&buf, uint64(len(dict))); err != nil {
		return nil, err
	}
	for _, value := range dict {
		if err := writeBytes(&buf, []byte(value)); err != nil {
			return nil, err
		}
	}
	if err := writeUvarint(&buf, uint64(len(values))); err != nil {
		return nil, err
	}
	for _, index := range indices {
		if err := writeUvarint(&buf, index); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeStringColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([]string, error) {
	block, err := decodeColumnBlock(meta, payloadOffset, data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	dictCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	dict := make([]string, 0, dictCount)
	for idx := 0; idx < int(dictCount); idx++ {
		raw, err := readBytes(reader)
		if err != nil {
			return nil, err
		}
		dict = append(dict, string(raw))
	}
	rowCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	return decodeSelectedStrings(reader, dict, int(rowCount), selected)
}

func decodeSelectedStrings(reader *bytes.Reader, dict []string, rowCount int, selected []int) ([]string, error) {
	if selected == nil {
		out := make([]string, 0, rowCount)
		for row := 0; row < rowCount; row++ {
			index, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, err
			}
			if index >= uint64(len(dict)) {
				return nil, fmt.Errorf("dictionary index %d out of bounds", index)
			}
			out = append(out, dict[index])
		}
		return out, nil
	}
	out := make([]string, 0, len(selected))
	selectedPos := 0
	for row := 0; row < rowCount; row++ {
		index, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}
		if index >= uint64(len(dict)) {
			return nil, fmt.Errorf("dictionary index %d out of bounds", index)
		}
		if selectedPos < len(selected) && row == selected[selectedPos] {
			out = append(out, dict[index])
			selectedPos++
		}
	}
	return out, nil
}

func encodeDeltaOfDelta(values []int64) []byte {
	var buf bytes.Buffer
	_ = writeUvarint(&buf, uint64(len(values)))
	if len(values) == 0 {
		return buf.Bytes()
	}
	var first [8]byte
	binary.LittleEndian.PutUint64(first[:], uint64(values[0]))
	_, _ = buf.Write(first[:])
	if len(values) == 1 {
		return buf.Bytes()
	}
	_ = writeVarint(&buf, values[1]-values[0])
	prevValue := values[1]
	prevDelta := values[1] - values[0]
	for _, value := range values[2:] {
		delta := value - prevValue
		_ = writeVarint(&buf, delta-prevDelta)
		prevDelta = delta
		prevValue = value
	}
	return buf.Bytes()
}

func decodeDeltaOfDeltaColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([]int64, error) {
	block, err := decodeColumnBlock(meta, payloadOffset, data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	rowCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if rowCount == 0 {
		return nil, nil
	}
	var first [8]byte
	if _, err := io.ReadFull(reader, first[:]); err != nil {
		return nil, err
	}
	values := make([]int64, 0, rowCount)
	current := int64(binary.LittleEndian.Uint64(first[:]))
	values = appendSelectedInt64(values, selected, 0, current)
	if rowCount == 1 {
		return values, nil
	}
	prevDelta, err := binary.ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	current += prevDelta
	values = appendSelectedInt64(values, selected, 1, current)
	for row := 2; row < int(rowCount); row++ {
		deltaOfDelta, err := binary.ReadVarint(reader)
		if err != nil {
			return nil, err
		}
		prevDelta += deltaOfDelta
		current += prevDelta
		values = appendSelectedInt64(values, selected, row, current)
	}
	return values, nil
}

func appendSelectedInt64(values []int64, selected []int, row int, value int64) []int64 {
	if selected == nil {
		return append(values, value)
	}
	for _, candidate := range selected {
		if candidate == row {
			return append(values, value)
		}
		if candidate > row {
			return values
		}
	}
	return values
}

func encodeBlobs(values [][]byte) []byte {
	var buf bytes.Buffer
	_ = writeUvarint(&buf, uint64(len(values)))
	for _, value := range values {
		_ = writeBytes(&buf, value)
	}
	return buf.Bytes()
}

func decodeBlobColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([][]byte, error) {
	block, err := decodeColumnBlock(meta, payloadOffset, data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	rowCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if selected == nil {
		out := make([][]byte, 0, rowCount)
		for row := 0; row < int(rowCount); row++ {
			value, err := readBytes(reader)
			if err != nil {
				return nil, err
			}
			out = append(out, cloneBytes(value))
		}
		return out, nil
	}
	out := make([][]byte, 0, len(selected))
	selectedPos := 0
	for row := 0; row < int(rowCount); row++ {
		value, err := readBytes(reader)
		if err != nil {
			return nil, err
		}
		if selectedPos < len(selected) && row == selected[selectedPos] {
			out = append(out, cloneBytes(value))
			selectedPos++
		}
	}
	return out, nil
}

func decodeColumnBlock(meta ColumnMeta, payloadOffset int, data []byte) ([]byte, error) {
	start := payloadOffset + meta.Offset
	end := start + meta.Size
	if start < 0 || end > len(data) || start > end {
		return nil, fmt.Errorf("column %s out of bounds", meta.Name)
	}
	switch meta.Compression {
	case "", columnCompressionGzip:
		return gzipDecompress(data[start:end])
	default:
		return nil, fmt.Errorf("unsupported compression %q", meta.Compression)
	}
}

func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		_ = zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func gzipDecompress(data []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	return io.ReadAll(zr)
}

func writeUvarint(buf *bytes.Buffer, value uint64) error {
	var raw [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(raw[:], value)
	_, err := buf.Write(raw[:n])
	return err
}

func writeVarint(buf *bytes.Buffer, value int64) error {
	var raw [binary.MaxVarintLen64]byte
	n := binary.PutVarint(raw[:], value)
	_, err := buf.Write(raw[:n])
	return err
}

func writeBytes(buf *bytes.Buffer, value []byte) error {
	if err := writeUvarint(buf, uint64(len(value))); err != nil {
		return err
	}
	_, err := buf.Write(value)
	return err
}

func readBytes(reader *bytes.Reader) ([]byte, error) {
	size, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, int(size))
	if _, err := io.ReadFull(reader, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	return append([]byte(nil), value...)
}

func encodeBlobDictionary(values [][]byte) ([]byte, error) {
	var buf bytes.Buffer
	dictIndex := make(map[string]uint64, len(values))
	dict := make([][]byte, 0)
	indices := make([]uint64, 0, len(values))
	for _, value := range values {
		key := string(value)
		index, ok := dictIndex[key]
		if !ok {
			index = uint64(len(dict))
			dictIndex[key] = index
			dict = append(dict, value)
		}
		indices = append(indices, index)
	}
	if err := writeUvarint(&buf, uint64(len(dict))); err != nil {
		return nil, err
	}
	for _, value := range dict {
		if err := writeBytes(&buf, value); err != nil {
			return nil, err
		}
	}
	if err := writeUvarint(&buf, uint64(len(values))); err != nil {
		return nil, err
	}
	for _, index := range indices {
		if err := writeUvarint(&buf, index); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func decodeBlobDictionaryColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([][]byte, error) {
	block, err := decodeColumnBlock(meta, payloadOffset, data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	dictCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	dict := make([][]byte, 0, dictCount)
	for idx := 0; idx < int(dictCount); idx++ {
		raw, err := readBytes(reader)
		if err != nil {
			return nil, err
		}
		dict = append(dict, raw)
	}
	rowCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if selected == nil {
		out := make([][]byte, 0, rowCount)
		for row := 0; row < int(rowCount); row++ {
			index, err := binary.ReadUvarint(reader)
			if err != nil {
				return nil, err
			}
			if index >= uint64(len(dict)) {
				return nil, fmt.Errorf("blob dictionary index %d out of bounds", index)
			}
			out = append(out, dict[index])
		}
		return out, nil
	}
	out := make([][]byte, 0, len(selected))
	selectedPos := 0
	for row := 0; row < int(rowCount); row++ {
		index, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}
		if index >= uint64(len(dict)) {
			return nil, fmt.Errorf("blob dictionary index %d out of bounds", index)
		}
		if selectedPos < len(selected) && row == selected[selectedPos] {
			out = append(out, dict[index])
			selectedPos++
		}
	}
	return out, nil
}

func encodeBooleansBitpack(values []bool) []byte {
	var buf bytes.Buffer
	_ = writeUvarint(&buf, uint64(len(values)))
	byteCount := (len(values) + 7) / 8
	packed := make([]byte, byteCount)
	for i, v := range values {
		if v {
			packed[i/8] |= 1 << (uint(i) % 8)
		}
	}
	_, _ = buf.Write(packed)
	return buf.Bytes()
}

func decodeBooleanBitpackColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([]bool, error) {
	block, err := decodeColumnBlock(meta, payloadOffset, data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	rowCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	byteCount := (int(rowCount) + 7) / 8
	packed := make([]byte, byteCount)
	if _, err := io.ReadFull(reader, packed); err != nil {
		return nil, err
	}
	if selected == nil {
		out := make([]bool, 0, rowCount)
		for row := 0; row < int(rowCount); row++ {
			out = append(out, packed[row/8]&(1<<(uint(row)%8)) != 0)
		}
		return out, nil
	}
	out := make([]bool, 0, len(selected))
	for _, row := range selected {
		if row >= int(rowCount) {
			break
		}
		out = append(out, packed[row/8]&(1<<(uint(row)%8)) != 0)
	}
	return out, nil
}

func encodeFloat64XOR(values []float64) []byte {
	var buf bytes.Buffer
	_ = writeUvarint(&buf, uint64(len(values)))
	if len(values) == 0 {
		return buf.Bytes()
	}
	first := math.Float64bits(values[0])
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], first)
	_, _ = buf.Write(raw[:])
	prev := first
	for _, v := range values[1:] {
		curr := math.Float64bits(v)
		binary.LittleEndian.PutUint64(raw[:], curr^prev)
		_, _ = buf.Write(raw[:])
		prev = curr
	}
	return buf.Bytes()
}

func decodeFloat64XORColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([]float64, error) {
	block, err := decodeColumnBlock(meta, payloadOffset, data)
	if err != nil {
		return nil, err
	}
	reader := bytes.NewReader(block)
	rowCount, err := binary.ReadUvarint(reader)
	if err != nil {
		return nil, err
	}
	if rowCount == 0 {
		return nil, nil
	}
	var raw [8]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return nil, err
	}
	prev := binary.LittleEndian.Uint64(raw[:])
	all := make([]float64, 0, rowCount)
	all = append(all, math.Float64frombits(prev))
	for row := 1; row < int(rowCount); row++ {
		if _, err := io.ReadFull(reader, raw[:]); err != nil {
			return nil, err
		}
		prev ^= binary.LittleEndian.Uint64(raw[:])
		all = append(all, math.Float64frombits(prev))
	}
	if selected == nil {
		return all, nil
	}
	out := make([]float64, 0, len(selected))
	for _, idx := range selected {
		if idx < len(all) {
			out = append(out, all[idx])
		}
	}
	return out, nil
}
