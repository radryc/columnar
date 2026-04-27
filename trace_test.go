package columnar

import (
	"encoding/json"
	"testing"
)

func TestTraceChunkRoundTripAndSelect(t *testing.T) {
	t.Parallel()

	records := []TraceRecord{
		{
			Tenant:        "tenant-a",
			TraceID:       "trace-1",
			SpanID:        "span-1",
			Name:          "GET /api",
			Kind:          "server",
			Service:       "api",
			StartUnixNano: 1000,
			EndUnixNano:   1500,
			StatusCode:    "ok",
			AttributesJSON: mustJSON(t, map[string]string{
				"http.method": "GET",
			}),
		},
		{
			Tenant:        "tenant-a",
			TraceID:       "trace-1",
			SpanID:        "span-2",
			ParentSpanID:  "span-1",
			Name:          "SELECT users",
			Kind:          "client",
			Service:       "api",
			StartUnixNano: 1100,
			EndUnixNano:   1300,
			StatusCode:    "ok",
			StatusMessage: "db query",
			EventsJSON:    mustJSON(t, []map[string]any{{"name": "cache_miss"}}),
			ResourceAttributesJSON: mustJSON(t, map[string]string{
				"host.name": "app-1",
			}),
		},
		{
			Tenant:        "tenant-a",
			TraceID:       "trace-2",
			SpanID:        "span-3",
			Name:          "POST /orders",
			Kind:          "server",
			Service:       "orders",
			StartUnixNano: 2000,
			EndUnixNano:   2600,
			StatusCode:    "error",
		},
	}

	data, header, err := EncodeTraceChunk(records)
	if err != nil {
		t.Fatalf("EncodeTraceChunk() error = %v", err)
	}
	if header.RowCount != len(records) {
		t.Fatalf("header row count = %d, want %d", header.RowCount, len(records))
	}
	if header.MinStartUnixNano != 1000 || header.MaxEndUnixNano != 2600 {
		t.Fatalf("header bounds = (%d, %d), want (1000, 2600)", header.MinStartUnixNano, header.MaxEndUnixNano)
	}

	decoded, decodedHeader, err := DecodeTraceChunk(data)
	if err != nil {
		t.Fatalf("DecodeTraceChunk() error = %v", err)
	}
	if decodedHeader.RowCount != len(records) {
		t.Fatalf("decoded header row count = %d, want %d", decodedHeader.RowCount, len(records))
	}
	if len(decoded) != len(records) {
		t.Fatalf("decoded rows = %d, want %d", len(decoded), len(records))
	}
	if decoded[1].ParentSpanID != "span-1" || string(decoded[1].EventsJSON) == "" {
		t.Fatalf("decoded record[1] = %+v", decoded[1])
	}

	filtered, _, err := SelectTraceByID(data, "trace-1")
	if err != nil {
		t.Fatalf("SelectTraceByID() error = %v", err)
	}
	if len(filtered) != 2 {
		t.Fatalf("filtered rows = %d, want 2", len(filtered))
	}
	for _, record := range filtered {
		if record.TraceID != "trace-1" {
			t.Fatalf("unexpected filtered record trace id %q", record.TraceID)
		}
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return raw
}
