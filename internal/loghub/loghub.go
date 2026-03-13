package loghub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-zeromq/zmq4"
)

const (
	defaultBot         = "zmq_manager"
	defaultExchange    = "zmq_manager"
	defaultLoggerName  = "zmq_manager"
	defaultQueueSize   = 1024
	defaultSocketHWM   = 1024
	defaultDialTimeout = 250 * time.Millisecond
	defaultSendTimeout = 10 * time.Millisecond
	defaultDialRetry   = 2 * time.Second
	envEndpoint        = "CENTRAL_LOG_ENDPOINT"
	envBot             = "CENTRAL_LOG_BOT"
	envExchange        = "CENTRAL_LOG_EXCHANGE"
	envQueueSize       = "CENTRAL_LOG_BUFFER"
	envSocketHWM       = "CENTRAL_LOG_SNDHWM"
	envLoggerName      = "CENTRAL_LOG_LOGGER"
)

type Config struct {
	Endpoint   string
	Bot        string
	Exchange   string
	LoggerName string
	QueueSize  int
	SocketHWM  int
}

type Event struct {
	EventID  string         `json:"event_id"`
	CorrID   string         `json:"corr_id"`
	TSMS     int64          `json:"ts_ms"`
	Level    string         `json:"level"`
	Logger   string         `json:"logger"`
	Msg      string         `json:"msg"`
	Filename string         `json:"filename"`
	Lineno   int            `json:"lineno"`
	Func     string         `json:"func"`
	Process  int            `json:"process"`
	Thread   int            `json:"thread"`
	Host     string         `json:"host"`
	Bot      string         `json:"bot"`
	Exchange string         `json:"exchange"`
	Symbol   *string        `json:"symbol"`
	Strategy *string        `json:"strategy"`
	Extra    map[string]any `json:"extra"`
	ExcText  *string        `json:"exc_text"`
}

type Writer struct {
	cfg     Config
	host    string
	process int
	queue   chan []byte
	cancel  context.CancelFunc
	done    chan struct{}
	seq     uint64
}

func NewFromEnv() (*Writer, error) {
	cfg := Config{
		Endpoint:   strings.TrimSpace(os.Getenv(envEndpoint)),
		Bot:        valueOrDefault(os.Getenv(envBot), defaultBot),
		Exchange:   valueOrDefault(os.Getenv(envExchange), defaultExchange),
		LoggerName: valueOrDefault(os.Getenv(envLoggerName), defaultLoggerName),
		QueueSize:  envIntOrDefault(envQueueSize, defaultQueueSize),
		SocketHWM:  envIntOrDefault(envSocketHWM, defaultSocketHWM),
	}
	if cfg.Endpoint == "" {
		return nil, nil
	}
	return New(cfg)
}

func New(cfg Config) (*Writer, error) {
	if strings.TrimSpace(cfg.Endpoint) == "" {
		return nil, nil
	}
	if cfg.Bot == "" {
		cfg.Bot = defaultBot
	}
	if cfg.Exchange == "" {
		cfg.Exchange = defaultExchange
	}
	if cfg.LoggerName == "" {
		cfg.LoggerName = defaultLoggerName
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.SocketHWM <= 0 {
		cfg.SocketHWM = defaultSocketHWM
	}

	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}

	ctx, cancel := context.WithCancel(context.Background())
	socket := zmq4.NewPush(
		ctx,
		zmq4.WithTimeout(defaultSendTimeout),
		zmq4.WithDialerTimeout(defaultDialTimeout),
		zmq4.WithDialerRetry(defaultDialRetry),
		zmq4.WithDialerMaxRetries(-1),
		zmq4.WithAutomaticReconnect(true),
	)
	_ = socket.SetOption(zmq4.OptionHWM, cfg.SocketHWM)
	if err := socket.Dial(cfg.Endpoint); err != nil {
		cancel()
		_ = socket.Close()
		return nil, err
	}

	w := &Writer{
		cfg:     cfg,
		host:    host,
		process: os.Getpid(),
		queue:   make(chan []byte, cfg.QueueSize),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go w.run(ctx, socket)
	return w, nil
}

func (w *Writer) Write(p []byte) (int, error) {
	if w == nil || len(p) == 0 {
		return len(p), nil
	}

	lines := bytes.Split(p, []byte{'\n'})
	for _, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		payload, ok := w.buildEvent(line)
		if !ok {
			continue
		}
		select {
		case w.queue <- payload:
		default:
		}
	}
	return len(p), nil
}

func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	w.cancel()
	select {
	case <-w.done:
	case <-time.After(100 * time.Millisecond):
	}
	return nil
}

func (w *Writer) run(ctx context.Context, socket zmq4.Socket) {
	defer close(w.done)
	defer socket.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case payload := <-w.queue:
			if len(payload) == 0 {
				continue
			}
			if err := socket.Send(zmq4.NewMsg(payload)); err != nil {
				continue
			}
		}
	}
}

func (w *Writer) buildEvent(line []byte) ([]byte, bool) {
	msg, tsMS := parseLogLine(string(line))
	if strings.TrimSpace(msg) == "" {
		return nil, false
	}

	eventID := fmt.Sprintf("%d-%d-%d", tsMS, w.process, atomic.AddUint64(&w.seq, 1))
	event := Event{
		EventID:  eventID,
		CorrID:   eventID,
		TSMS:     tsMS,
		Level:    detectLevel(msg),
		Logger:   w.cfg.LoggerName,
		Msg:      msg,
		Filename: "",
		Lineno:   0,
		Func:     "",
		Process:  w.process,
		Thread:   0,
		Host:     w.host,
		Bot:      w.cfg.Bot,
		Exchange: w.cfg.Exchange,
		Symbol:   nil,
		Strategy: nil,
		Extra: map[string]any{
			"source": "go",
		},
		ExcText: nil,
	}

	raw, err := json.Marshal(event)
	if err != nil {
		return nil, false
	}
	return raw, true
}

func parseLogLine(line string) (string, int64) {
	line = strings.TrimSpace(line)
	now := time.Now().UnixMilli()
	if len(line) < 20 {
		return line, now
	}

	prefix := line[:19]
	ts, err := time.ParseInLocation("2006/01/02 15:04:05", prefix, time.Local)
	if err != nil {
		return line, now
	}

	msg := strings.TrimSpace(line[19:])
	return msg, ts.UnixMilli()
}

func detectLevel(msg string) string {
	upper := strings.ToUpper(msg)
	switch {
	case strings.Contains(upper, "PANIC"), strings.Contains(upper, "[FATAL]"), strings.Contains(upper, "-FATAL]"), strings.HasPrefix(upper, "FATAL"):
		return "FATAL"
	case strings.Contains(upper, "[ERROR]"), strings.Contains(upper, "-ERROR]"), strings.Contains(upper, " ERROR "):
		return "ERROR"
	case strings.Contains(upper, "[WARN]"), strings.Contains(upper, "-WARN]"), strings.Contains(upper, " WARNING "), strings.Contains(upper, " WARN "):
		return "WARN"
	default:
		return "INFO"
	}
}

func envIntOrDefault(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	var v int
	_, err := fmt.Sscanf(raw, "%d", &v)
	if err != nil || v <= 0 {
		return fallback
	}
	return v
}

func valueOrDefault(v, fallback string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return fallback
	}
	return v
}

var _ io.WriteCloser = (*Writer)(nil)
