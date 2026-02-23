package broker

import (
	"context"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
	json "github.com/goccy/go-json"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	pingInterval  = 15 * time.Second
	clientTimeout = 45 * time.Second
	// zmqBindAddress = "tcp://*:5555"
	HeaderJSON     = "J"
	HeaderTradeBin = "T" // Trades Binary (MsgPack)
	HeaderOBBin    = "O" // OrderBooks Binary (MsgPack)
)

type Client struct {
	ID       []byte
	LastPong time.Time
	Encoding string // "json" or "msgpack"
}

type ClientManager struct {
	socket         zmq4.Socket
	clients        map[string]*Client
	clientsMu      sync.RWMutex
	requestCh      chan<- *shared_types.ClientRequest
	DistributionCh chan *DistributionMessage
	sendCh         chan zmq4.Msg
	bindAddress    string
}

type incomingClientMessage struct {
	Action         string   `json:"action"`
	Exchange       string   `json:"exchange"`
	Symbol         string   `json:"symbol"`
	Symbols        []string `json:"symbols"`
	MarketType     string   `json:"market_type"`
	DataType       string   `json:"data_type"`
	OrderBookDepth int      `json:"depth,omitempty"`
	Encoding       string   `json:"encoding"`
	Message        string   `json:"message"`
}

func NewClientManager(requestCh chan<- *shared_types.ClientRequest) *ClientManager {
	socket := zmq4.NewRouter(context.Background(), zmq4.WithID([]byte("broker-router")))

	var bindAddr string
	if runtime.GOOS == "linux" {
		bindAddr = "ipc:///tmp/feed_broker.ipc"
		log.Println("[NETWORK] Linux erkannt: Nutze High-Performance IPC (ipc:///tmp/feed_broker.ipc)")
	} else {
		bindAddr = "tcp://*:5555"
		log.Println("[NETWORK] Windows/Mac erkannt: Nutze Standard TCP (tcp://*:5555)")
	}

	if strings.HasPrefix(bindAddr, "ipc://") {
		path := strings.TrimPrefix(bindAddr, "ipc://")
		_ = os.Remove(path)
	}

	if err := socket.Listen(bindAddr); err != nil {
		log.Fatalf("[ZMQ-FATAL] Socket konnte nicht gebunden werden auf %s: %v", bindAddr, err)
	}

	return &ClientManager{
		socket:         socket,
		clients:        make(map[string]*Client),
		requestCh:      requestCh,
		DistributionCh: make(chan *DistributionMessage, 10000),
		sendCh:         make(chan zmq4.Msg, 4096),
		bindAddress:    bindAddr,
	}
}

func (cm *ClientManager) Run() {
	log.Println("[CLIENT-MANAGER] Startet...")
	log.Printf("[CLIENT-MANAGER] Lauscht auf ZMQ-Anfragen unter %s", cm.bindAddress)

	go cm.readLoop()
	go cm.distributionLoop()
	go cm.writeLoop()
	go cm.healthCheckLoop()
}

func (cm *ClientManager) readLoop() {
	for {
		msg, err := cm.socket.Recv()
		if err != nil {
			log.Printf("[ZMQ-RECV-ERROR] %v", err)
			continue
		}

		var clientID, payload []byte
		if len(msg.Frames) >= 2 {
			clientID = msg.Frames[0]
			payload = msg.Frames[len(msg.Frames)-1]
		} else {
			continue
		}

		clientIDStr := string(clientID)
		cm.clientsMu.Lock()
		if _, ok := cm.clients[clientIDStr]; !ok {
			log.Printf("[CLIENT-MANAGER] Neuer Client verbunden: %s", clientIDStr)
			cm.clients[clientIDStr] = &Client{
				ID:       append([]byte(nil), clientID...),
				LastPong: time.Now(),
				Encoding: "json",
			}
		}
		cm.clientsMu.Unlock()

		cm.handleMessage(clientID, payload)
	}
}

func (cm *ClientManager) distributionLoop() {
	for msg := range cm.DistributionCh {
		var jsonCache []byte
		var msgpackCache []byte

		var msgTypeHeader string
		switch msg.RawPayload.(type) {
		case []*shared_types.TradeUpdate:
			msgTypeHeader = HeaderTradeBin
		case []*shared_types.OrderBookUpdate:
			msgTypeHeader = HeaderOBBin
		default:
			continue
		}

		for _, clientID := range msg.ClientIDs {
			clientIDStr := string(clientID)

			cm.clientsMu.RLock()
			client, exists := cm.clients[clientIDStr]
			encoding := ""
			if exists {
				encoding = client.Encoding
			}
			cm.clientsMu.RUnlock()
			if !exists {
				continue
			}

			if encoding == "msgpack" || encoding == "binary" {
				if msgpackCache == nil {
					var err error
					msgpackCache, err = msgpack.Marshal(msg.RawPayload)
					if err != nil {
						log.Printf("[MSGPACK ERROR] %v", err)
						continue
					}
				}

				cm.enqueueSocketSend(zmq4.NewMsgFrom(
					clientID,
					[]byte(msgTypeHeader),
					msgpackCache,
				))
			} else {
				if jsonCache == nil {
					var err error
					jsonCache, err = json.Marshal(msg.RawPayload)
					if err != nil {
						continue
					}
				}
				cm.enqueueSocketSend(zmq4.NewMsgFrom(clientID, jsonCache))
			}
		}

		if msg.OnComplete != nil {
			msg.OnComplete()
		}
	}
}

func (cm *ClientManager) writeLoop() {
	for outbound := range cm.sendCh {
		_ = cm.socket.Send(outbound)
	}
}

func (cm *ClientManager) healthCheckLoop() {
	type clientSnapshot struct {
		id       string
		clientID []byte
		lastPong time.Time
	}

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		pingPayload, _ := json.Marshal(map[string]string{"type": "ping"})
		clientsToRemove := make([]string, 0)

		cm.clientsMu.RLock()
		snapshots := make([]clientSnapshot, 0, len(cm.clients))
		for id, client := range cm.clients {
			snapshots = append(snapshots, clientSnapshot{
				id:       id,
				clientID: append([]byte(nil), client.ID...),
				lastPong: client.LastPong,
			})
		}
		cm.clientsMu.RUnlock()

		for _, client := range snapshots {
			if now.Sub(client.lastPong) > clientTimeout {
				clientsToRemove = append(clientsToRemove, client.id)
				continue
			}
			cm.enqueueSocketSend(zmq4.NewMsgFrom(client.clientID, []byte{}, pingPayload))
		}

		for _, id := range clientsToRemove {
			var removedClientID []byte

			cm.clientsMu.Lock()
			if client, ok := cm.clients[id]; ok {
				if now.Sub(client.LastPong) > clientTimeout {
					removedClientID = append([]byte(nil), client.ID...)
					delete(cm.clients, id)
				}
			}
			cm.clientsMu.Unlock()

			if len(removedClientID) == 0 {
				continue
			}

			log.Printf("[CLIENT-MANAGER] Client %s wegen Timeout entfernt.", id)
			cm.enqueueRequest(&shared_types.ClientRequest{
				ClientID: removedClientID,
				Action:   "disconnect",
			})
		}
	}
}

func (cm *ClientManager) handleMessage(clientID []byte, payload []byte) {
	var req incomingClientMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		cm.sendClientError(clientID, "invalid_json", "payload must be valid JSON")
		return
	}

	clientIDStr := string(clientID)

	if req.Message == "pong" {
		cm.clientsMu.Lock()
		if client, exists := cm.clients[clientIDStr]; exists {
			client.LastPong = time.Now()
		}
		cm.clientsMu.Unlock()
		return
	}

	if req.Encoding != "" {
		encoding, ok := normalizeClientEncoding(req.Encoding)
		if !ok {
			cm.sendClientError(clientID, "invalid_encoding", "encoding must be one of: json, msgpack, binary")
			return
		}

		cm.clientsMu.Lock()
		if client, exists := cm.clients[clientIDStr]; exists {
			client.Encoding = encoding
		}
		cm.clientsMu.Unlock()

		log.Printf("[CLIENT-MANAGER] Client %s setzt Encoding auf: %s", clientIDStr, encoding)
	}

	if req.Action == "" {
		if req.Encoding == "" {
			cm.sendClientError(clientID, "missing_action", "action field is required")
		}
		return
	}

	if req.DataType == "" {
		req.DataType = "trades"
	}

	switch req.Action {
	case "subscribe_bulk":
		if req.Exchange == "" || req.MarketType == "" || len(req.Symbols) == 0 {
			cm.sendClientError(clientID, "invalid_request", "subscribe_bulk requires exchange, market_type and symbols")
			return
		}

		log.Printf("[CLIENT-MANAGER] 'subscribe_bulk': %d Symbole von %s", len(req.Symbols), clientIDStr)
		for _, symbol := range req.Symbols {
			if symbol == "" {
				continue
			}
			cm.enqueueRequest(&shared_types.ClientRequest{
				ClientID:       clientID,
				Action:         "subscribe",
				Exchange:       req.Exchange,
				Symbol:         symbol,
				MarketType:     req.MarketType,
				DataType:       req.DataType,
				OrderBookDepth: req.OrderBookDepth,
			})
		}
		return

	case "subscribe_all":
		if req.Exchange == "" || req.MarketType == "" {
			cm.sendClientError(clientID, "invalid_request", "subscribe_all requires exchange and market_type")
			return
		}

	case "subscribe", "unsubscribe":
		if req.Exchange == "" || req.MarketType == "" || req.Symbol == "" {
			cm.sendClientError(clientID, "invalid_request", "subscribe/unsubscribe requires exchange, symbol and market_type")
			return
		}

	case "disconnect":
		cm.enqueueRequest(&shared_types.ClientRequest{
			ClientID: clientID,
			Action:   "disconnect",
		})
		return

	default:
		cm.sendClientError(clientID, "unknown_action", "unsupported action")
		return
	}

	cm.enqueueRequest(&shared_types.ClientRequest{
		ClientID:       clientID,
		Action:         req.Action,
		Exchange:       req.Exchange,
		Symbol:         req.Symbol,
		MarketType:     req.MarketType,
		DataType:       req.DataType,
		OrderBookDepth: req.OrderBookDepth,
	})
}

func (cm *ClientManager) enqueueRequest(req *shared_types.ClientRequest) {
	if cm.requestCh == nil {
		return
	}
	cm.requestCh <- req
}

func (cm *ClientManager) sendClientError(clientID []byte, code, message string) {
	payload, err := json.Marshal(map[string]string{
		"type":    "error",
		"code":    code,
		"message": message,
	})
	if err != nil {
		return
	}
	cm.enqueueSocketSend(zmq4.NewMsgFrom(clientID, payload))
}

func normalizeClientEncoding(input string) (string, bool) {
	switch strings.ToLower(input) {
	case "json":
		return "json", true
	case "msgpack", "binary":
		return "msgpack", true
	default:
		return "", false
	}
}

func (cm *ClientManager) enqueueSocketSend(msg zmq4.Msg) {
	select {
	case cm.sendCh <- msg:
	default:
	}
}
