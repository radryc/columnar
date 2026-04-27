package columnar

import "testing"

func TestMetricChunkRoundTripAndSelect(t *testing.T) {
	t.Parallel()

	records := []MetricPointRecord{
		{
			Tenant:            "tenant-a",
			Service:           "api",
			MetricName:        "http.duration_ms",
			Type:              "gauge",
			TimestampUnixNano: 1000,
			StartUnixNano:     900,
			HasValue:          true,
			Value:             12.5,
		},
		{
			Tenant:            "tenant-a",
			Service:           "api",
			MetricName:        "http.duration_ms",
			Type:              "histogram",
			TimestampUnixNano: 2000,
			StartUnixNano:     1900,
			RawJSON:           mustJSON(t, map[string]any{"count": 3}),
		},
		{
			Tenant:            "tenant-a",
			Service:           "api",
			MetricName:        "queue.depth",
			Type:              "gauge",
			TimestampUnixNano: 3000,
			HasValue:          true,
			Value:             7,
		},
	}

	data, header, err := EncodeMetricChunk(records)
	if err != nil {
		t.Fatalf("EncodeMetricChunk() error = %v", err)
	}
	if header.RowCount != 3 {
		t.Fatalf("header.RowCount = %d, want 3", header.RowCount)
	}

	decoded, _, err := DecodeMetricChunk(data)
	if err != nil {
		t.Fatalf("DecodeMetricChunk() error = %v", err)
	}
	if len(decoded) != 3 {
		t.Fatalf("decoded rows = %d, want 3", len(decoded))
	}
	if !decoded[0].HasValue || decoded[0].Value != 12.5 {
		t.Fatalf("decoded[0] = %+v", decoded[0])
	}

	selected, _, err := SelectMetricChunk(data, "http.duration_ms", 0, 2500)
	if err != nil {
		t.Fatalf("SelectMetricChunk() error = %v", err)
	}
	if len(selected) != 2 {
		t.Fatalf("selected rows = %d, want 2", len(selected))
	}
	if selected[1].Type != "histogram" || len(selected[1].RawJSON) == 0 {
		t.Fatalf("selected[1] = %+v", selected[1])
	}
}
