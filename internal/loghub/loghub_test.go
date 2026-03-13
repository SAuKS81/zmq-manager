package loghub

import (
	"encoding/json"
	"testing"
)

func TestParseLogLineStripsStdTimestamp(t *testing.T) {
	msg, ts := parseLogLine("2026/03/13 12:34:56 Broker laeuft.")
	if msg != "Broker laeuft." {
		t.Fatalf("unexpected message: %q", msg)
	}
	if ts == 0 {
		t.Fatal("expected parsed timestamp")
	}
}

func TestDetectLevel(t *testing.T) {
	cases := map[string]string{
		"[CCXT-BATCH-SHARD-ERROR] boom": "ERROR",
		"[CLIENT-MANAGER-WARN] slow":    "WARN",
		"panic: concurrent map writes":  "FATAL",
		"Broker laeuft.":                "INFO",
	}
	for input, want := range cases {
		if got := detectLevel(input); got != want {
			t.Fatalf("detectLevel(%q)=%q want=%q", input, got, want)
		}
	}
}

func TestWriterBuildEventUsesDefaults(t *testing.T) {
	w := &Writer{
		cfg: Config{
			Bot:        "zmq_manager",
			Exchange:   "zmq_manager",
			LoggerName: "zmq_manager",
		},
		host:    "test-host",
		process: 123,
	}

	raw, ok := w.buildEvent([]byte("2026/03/13 12:34:56 [SUB-MANAGER] Startet"))
	if !ok {
		t.Fatal("expected event payload")
	}

	var event Event
	if err := json.Unmarshal(raw, &event); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if event.Level != "INFO" {
		t.Fatalf("unexpected level: %s", event.Level)
	}
	if event.Logger != "zmq_manager" || event.Bot != "zmq_manager" || event.Exchange != "zmq_manager" {
		t.Fatalf("unexpected routing fields: %+v", event)
	}
	if event.Msg != "[SUB-MANAGER] Startet" {
		t.Fatalf("unexpected msg: %q", event.Msg)
	}
	if got := event.Extra["source"]; got != "go" {
		t.Fatalf("unexpected extra.source: %v", got)
	}
}
