package columnar

import (
	"bytes"
	"encoding/binary"
	"math"
)

const (
	metricMagic       = "AICOLMT1"
	metricKind        = "metric_chunk"
	metricFormatMajor = 1
)

type MetricPointRecord struct {
	Tenant                 string
	Service                string
	MetricName             string
	Description            string
	Unit                   string
	Type                   string
	TimestampUnixNano      int64
	StartUnixNano          int64
	Temporality            string
	AttributesJSON         []byte
	ResourceAttributesJSON []byte
	HasValue               bool
	Value                  float64
	RawJSON                []byte
}

type MetricChunkHeader struct {
	Version              int          `json:"version"`
	Kind                 string       `json:"kind"`
	RowCount             int          `json:"row_count"`
	MinTimestampUnixNano int64        `json:"min_timestamp_unix_nano"`
	MaxTimestampUnixNano int64        `json:"max_timestamp_unix_nano"`
	Columns              []ColumnMeta `json:"columns"`
}

func EncodeMetricChunk(records []MetricPointRecord) ([]byte, MetricChunkHeader, error) {
	columns := []struct {
		name     string
		encoding string
		values   func([]MetricPointRecord) ([]byte, error)
	}{
		{name: "tenant", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.Tenant }))
		}},
		{name: "service", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.Service }))
		}},
		{name: "metric_name", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.MetricName }))
		}},
		{name: "description", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.Description }))
		}},
		{name: "unit", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.Unit }))
		}},
		{name: "type", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.Type }))
		}},
		{name: "timestamp_unix_nano", encoding: columnEncodingDeltaOfDelta, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeDeltaOfDelta(metricInt64s(records, func(record MetricPointRecord) int64 { return record.TimestampUnixNano })), nil
		}},
		{name: "start_unix_nano", encoding: columnEncodingDeltaOfDelta, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeDeltaOfDelta(metricInt64s(records, func(record MetricPointRecord) int64 { return record.StartUnixNano })), nil
		}},
		{name: "temporality", encoding: columnEncodingDictionary, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeStringDictionary(metricStrings(records, func(record MetricPointRecord) string { return record.Temporality }))
		}},
		{name: "attributes", encoding: columnEncodingDictBlob, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeBlobDictionary(metricBlobs(records, func(record MetricPointRecord) []byte { return record.AttributesJSON }))
		}},
		{name: "resource_attributes", encoding: columnEncodingDictBlob, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeBlobDictionary(metricBlobs(records, func(record MetricPointRecord) []byte { return record.ResourceAttributesJSON }))
		}},
		{name: "has_value", encoding: columnEncodingBitpack, values: func(records []MetricPointRecord) ([]byte, error) {
			values := make([]bool, 0, len(records))
			for _, record := range records {
				values = append(values, record.HasValue)
			}
			return encodeBooleansBitpack(values), nil
		}},
		{name: "value", encoding: columnEncodingFloatXOR, values: func(records []MetricPointRecord) ([]byte, error) {
			values := make([]float64, 0, len(records))
			for _, record := range records {
				values = append(values, record.Value)
			}
			return encodeFloat64XOR(values), nil
		}},
		{name: "raw", encoding: columnEncodingDictBlob, values: func(records []MetricPointRecord) ([]byte, error) {
			return encodeBlobDictionary(metricBlobs(records, func(record MetricPointRecord) []byte { return record.RawJSON }))
		}},
	}

	header := MetricChunkHeader{
		Version:  metricFormatMajor,
		Kind:     metricKind,
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

	return encodeChunk(metricMagic, header, columns, records)
}

func DecodeMetricChunk(data []byte) ([]MetricPointRecord, MetricChunkHeader, error) {
	return decodeSelectedMetrics(data, nil)
}

func SelectMetricChunk(data []byte, metricName string, fromUnixNano, toUnixNano int64) ([]MetricPointRecord, MetricChunkHeader, error) {
	header, payloadOffset, columns, err := readChunk[MetricChunkHeader](data, metricMagic)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}

	names, err := decodeStringColumn(columns["metric_name"], payloadOffset, data, nil)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	timestamps, err := decodeDeltaOfDeltaColumn(columns["timestamp_unix_nano"], payloadOffset, data, nil)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	selected := make([]int, 0, len(names))
	for idx, name := range names {
		if metricName != "" && name != metricName {
			continue
		}
		if fromUnixNano > 0 && timestamps[idx] < fromUnixNano {
			continue
		}
		if toUnixNano > 0 && timestamps[idx] > toUnixNano {
			continue
		}
		selected = append(selected, idx)
	}
	if len(selected) == 0 {
		return nil, header, nil
	}
	return decodeSelectedMetrics(data, selected)
}

func decodeSelectedMetrics(data []byte, selected []int) ([]MetricPointRecord, MetricChunkHeader, error) {
	header, payloadOffset, columns, err := readChunk[MetricChunkHeader](data, metricMagic)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}

	tenants, err := decodeStringColumn(columns["tenant"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	services, err := decodeStringColumn(columns["service"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	names, err := decodeStringColumn(columns["metric_name"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	descriptions, err := decodeStringColumn(columns["description"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	units, err := decodeStringColumn(columns["unit"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	types, err := decodeStringColumn(columns["type"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	timestamps, err := decodeDeltaOfDeltaColumn(columns["timestamp_unix_nano"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	starts, err := decodeDeltaOfDeltaColumn(columns["start_unix_nano"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	temporalities, err := decodeStringColumn(columns["temporality"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	attrs, err := decodeBlobDictionaryColumn(columns["attributes"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	resourceAttrs, err := decodeBlobDictionaryColumn(columns["resource_attributes"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	hasValues, err := decodeBooleanBitpackColumn(columns["has_value"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	values, err := decodeFloat64XORColumn(columns["value"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}
	rawValues, err := decodeBlobDictionaryColumn(columns["raw"], payloadOffset, data, selected)
	if err != nil {
		return nil, MetricChunkHeader{}, err
	}

	out := make([]MetricPointRecord, 0, len(timestamps))
	for idx := range timestamps {
		out = append(out, MetricPointRecord{
			Tenant:                 tenants[idx],
			Service:                services[idx],
			MetricName:             names[idx],
			Description:            descriptions[idx],
			Unit:                   units[idx],
			Type:                   types[idx],
			TimestampUnixNano:      timestamps[idx],
			StartUnixNano:          starts[idx],
			Temporality:            temporalities[idx],
			AttributesJSON:         cloneBytes(attrs[idx]),
			ResourceAttributesJSON: cloneBytes(resourceAttrs[idx]),
			HasValue:               hasValues[idx],
			Value:                  values[idx],
			RawJSON:                cloneBytes(rawValues[idx]),
		})
	}
	return out, header, nil
}

func metricStrings(records []MetricPointRecord, pick func(MetricPointRecord) string) []string {
	out := make([]string, 0, len(records))
	for _, record := range records {
		out = append(out, pick(record))
	}
	return out
}

func metricInt64s(records []MetricPointRecord, pick func(MetricPointRecord) int64) []int64 {
	out := make([]int64, 0, len(records))
	for _, record := range records {
		out = append(out, pick(record))
	}
	return out
}

func metricBlobs(records []MetricPointRecord, pick func(MetricPointRecord) []byte) [][]byte {
	out := make([][]byte, 0, len(records))
	for _, record := range records {
		out = append(out, cloneBytes(pick(record)))
	}
	return out
}

func encodeBooleans(values []bool) []byte {
	var buf bytes.Buffer
	_ = writeUvarint(&buf, uint64(len(values)))
	for _, value := range values {
		var raw byte
		if value {
			raw = 1
		}
		_ = buf.WriteByte(raw)
	}
	return buf.Bytes()
}

func decodeBooleanColumn(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([]bool, error) {
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
		out := make([]bool, 0, rowCount)
		for row := 0; row < int(rowCount); row++ {
			value, err := reader.ReadByte()
			if err != nil {
				return nil, err
			}
			out = append(out, value == 1)
		}
		return out, nil
	}
	out := make([]bool, 0, len(selected))
	selectedPos := 0
	for row := 0; row < int(rowCount); row++ {
		value, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if selectedPos < len(selected) && row == selected[selectedPos] {
			out = append(out, value == 1)
			selectedPos++
		}
	}
	return out, nil
}

func encodeFloat64s(values []float64) []byte {
	var buf bytes.Buffer
	_ = writeUvarint(&buf, uint64(len(values)))
	for _, value := range values {
		var raw [8]byte
		binary.LittleEndian.PutUint64(raw[:], math.Float64bits(value))
		_, _ = buf.Write(raw[:])
	}
	return buf.Bytes()
}

func decodeFloat64Column(meta ColumnMeta, payloadOffset int, data []byte, selected []int) ([]float64, error) {
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
		out := make([]float64, 0, rowCount)
		for row := 0; row < int(rowCount); row++ {
			var raw [8]byte
			if _, err := reader.Read(raw[:]); err != nil {
				return nil, err
			}
			out = append(out, math.Float64frombits(binary.LittleEndian.Uint64(raw[:])))
		}
		return out, nil
	}
	out := make([]float64, 0, len(selected))
	selectedPos := 0
	for row := 0; row < int(rowCount); row++ {
		var raw [8]byte
		if _, err := reader.Read(raw[:]); err != nil {
			return nil, err
		}
		if selectedPos < len(selected) && row == selected[selectedPos] {
			out = append(out, math.Float64frombits(binary.LittleEndian.Uint64(raw[:])))
			selectedPos++
		}
	}
	return out, nil
}
