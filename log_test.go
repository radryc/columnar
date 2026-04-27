package columnar

import "testing"

func TestLogChunkRoundTripAndSearch(t *testing.T) {
	t.Parallel()

	records := []LogRecord{
		{
			Tenant:            "tenant-a",
			Service:           "api",
			TimestampUnixNano: 1000,
			SeverityNumber:    9,
			SeverityText:      "INFO",
			Body:              "request started",
			TraceID:           "trace-1",
		},
		{
			Tenant:                 "tenant-a",
			Service:                "api",
			TimestampUnixNano:      2000,
			SeverityNumber:         17,
			SeverityText:           "ERROR",
			Body:                   "request failed with timeout",
			TraceID:                "trace-2",
			ResourceAttributesJSON: mustJSON(t, map[string]string{"service.name": "api"}),
			AttributesJSON:         mustJSON(t, map[string]string{"error.type": "timeout"}),
		},
	}

	data, header, err := EncodeLogChunk(records)
	if err != nil {
		t.Fatalf("EncodeLogChunk() error = %v", err)
	}
	if header.RowCount != 2 {
		t.Fatalf("header.RowCount = %d, want 2", header.RowCount)
	}

	decoded, _, err := DecodeLogChunk(data)
	if err != nil {
		t.Fatalf("DecodeLogChunk() error = %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded rows = %d, want 2", len(decoded))
	}
	if decoded[1].Body != "request failed with timeout" {
		t.Fatalf("decoded body = %q", decoded[1].Body)
	}

	matched, _, err := SearchLogChunk(data, LogSearchOptions{
		Query: "timeout",
		Limit: 10,
	})
	if err != nil {
		t.Fatalf("SearchLogChunk() error = %v", err)
	}
	if len(matched) != 1 {
		t.Fatalf("matched rows = %d, want 1", len(matched))
	}
	if matched[0].TraceID != "trace-2" {
		t.Fatalf("matched trace_id = %q, want trace-2", matched[0].TraceID)
	}
}
