package broker

import (
	"context"
	// "encoding/gob" <-- GOB RAUS
	
	// MessagePack & Turbo JSON REIN
	"github.com/vmihailenco/msgpack/v5"
	json "github.com/goccy/go-json"

	"log"
	"runtime"
	"time"

    "os"      // <--- NEU
    "strings" // <--- NEU (falls nicht schon da)

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
)

const (
	pingInterval   = 15 * time.Second
	clientTimeout  = 45 * time.Second
	// zmqBindAddress = "tcp://*:5555"
	HeaderJSON       = "J" 
	HeaderTradeBin   = "T" // Trades Binary (MsgPack)
	HeaderOBBin      = "O" // OrderBooks Binary (MsgPack)
)

type Client struct {
	ID       []byte
	LastPong time.Time
	Encoding string // "json" oder "msgpack"
}

type ClientManager struct {
	socket         zmq4.Socket
	clients        map[string]*Client
	requestCh      chan<- *shared_types.ClientRequest
	DistributionCh chan *DistributionMessage
	bindAddress    string
}

func NewClientManager(requestCh chan<- *shared_types.ClientRequest) *ClientManager {
	socket := zmq4.NewRouter(context.Background(), zmq4.WithID([]byte("broker-router")))

	// --- NEU: Plattform-abhängiges Binding ---
	var bindAddr string
	if runtime.GOOS == "linux" {
		// Auf Linux nutzen wir IPC (UNIX Domain Sockets).
		// Das umgeht den kompletten TCP/IP Stack -> Viel weniger CPU!
		bindAddr = "ipc:///tmp/feed_broker.ipc"
		log.Println("[NETWORK] Linux erkannt: Nutze High-Performance IPC (ipc:///tmp/feed_broker.ipc)")
	} else {
		// Auf Windows (und Mac zur Sicherheit) nutzen wir TCP.
		bindAddr = "tcp://*:5555"
		log.Println("[NETWORK] Windows/Mac erkannt: Nutze Standard TCP (tcp://*:5555)")
	}
	// -----------------------------------------

	if strings.HasPrefix(bindAddr, "ipc://") {
		path := strings.TrimPrefix(bindAddr, "ipc://")
		// Versuche Datei zu löschen. Fehler ignorieren (z.B. wenn Datei nicht existiert).
		_ = os.Remove(path) 
	}
	
	err := socket.Listen(bindAddr)
	if err != nil {
		log.Fatalf("[ZMQ-FATAL] Socket konnte nicht gebunden werden auf %s: %v", bindAddr, err)
	}

	return &ClientManager{
		socket:         socket,
		clients:        make(map[string]*Client),
		requestCh:      requestCh,
		DistributionCh: make(chan *DistributionMessage, 10000),
		bindAddress:    bindAddr,
	}
}

func (cm *ClientManager) Run() {
	log.Println("[CLIENT-MANAGER] Startet...")
	log.Printf("[CLIENT-MANAGER] Lauscht auf ZMQ-Anfragen unter %s", cm.bindAddress)

	go cm.readLoop()
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
		if _, ok := cm.clients[clientIDStr]; !ok {
			log.Printf("[CLIENT-MANAGER] Neuer Client verbunden: %s", clientIDStr)
			cm.clients[clientIDStr] = &Client{ID: clientID, LastPong: time.Now(), Encoding: "json"}
		}

		cm.handleMessage(clientID, payload)
	}
}

func (cm *ClientManager) writeLoop() {
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
			client, exists := cm.clients[clientIDStr]
			if !exists {
				continue
			}

			if client.Encoding == "msgpack" || client.Encoding == "binary" {
				// --- MSGPACK PFAD (High Performance) ---
				
				if msgpackCache == nil {
					var err error
					// MsgPack ist stateless und extrem schnell
					msgpackCache, err = msgpack.Marshal(msg.RawPayload)
					if err != nil {
						log.Printf("[MSGPACK ERROR] %v", err)
						continue
					}
				}
				
				cm.socket.Send(zmq4.NewMsgFrom(
					clientID,           
					[]byte(msgTypeHeader), 
					msgpackCache,           
				))

			} else {
				// --- JSON PFAD (goccy ist hier sehr schnell) ---
				if jsonCache == nil {
					var err error
					jsonCache, err = json.Marshal(msg.RawPayload)
					if err != nil { continue }
				}
				cm.socket.Send(zmq4.NewMsgFrom(clientID, jsonCache))
			}
		}

		if msg.OnComplete != nil {
			msg.OnComplete()
		}
	}
}

func (cm *ClientManager) healthCheckLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		pingPayload, _ := json.Marshal(map[string]string{"type": "ping"})
		var clientsToRemove []string

		for id, client := range cm.clients {
			if now.Sub(client.LastPong) > clientTimeout {
				clientsToRemove = append(clientsToRemove, id)
				continue
			}
			pingMsg := zmq4.NewMsgFrom(client.ID, []byte{}, pingPayload)
			cm.socket.Send(pingMsg)
		}

		for _, id := range clientsToRemove {
			log.Printf("[CLIENT-MANAGER] Client %s wegen Timeout entfernt.", id)
			cm.requestCh <- &shared_types.ClientRequest{
				ClientID: cm.clients[id].ID,
				Action:   "disconnect",
			}
			delete(cm.clients, id)
		}
	}
}

func (cm *ClientManager) handleMessage(clientID []byte, payload []byte) {
	var pongCheck map[string]string
	if err := json.Unmarshal(payload, &pongCheck); err == nil {
		if msg, ok := pongCheck["message"]; ok && msg == "pong" {
			if client, exists := cm.clients[string(clientID)]; exists {
				client.LastPong = time.Now()
			}
			return
		}
	}

	var genericReq map[string]interface{}
	if err := json.Unmarshal(payload, &genericReq); err != nil {
		return
	}

	if encodingVal, ok := genericReq["encoding"]; ok {
		if encodingStr, ok := encodingVal.(string); ok {
			if client, exists := cm.clients[string(clientID)]; exists {
				client.Encoding = encodingStr
				log.Printf("[CLIENT-MANAGER] Client %s setzt Encoding auf: %s", string(clientID), encodingStr)
			}
		}
	}
	
	actionField, ok := genericReq["action"]
	if !ok { return }
	action := actionField.(string)

	if action == "subscribe_bulk" {
		var bulkReq shared_types.BulkClientRequest
		json.Unmarshal(payload, &bulkReq)
		
		log.Printf("[CLIENT-MANAGER] 'subscribe_bulk': %d Symbole von %s", len(bulkReq.Symbols), string(clientID))

		for _, symbol := range bulkReq.Symbols {
			singleReq := &shared_types.ClientRequest{
				ClientID:       clientID,
				Action:         "subscribe",
				Exchange:       bulkReq.Exchange,
				Symbol:         symbol,
				MarketType:     bulkReq.MarketType,
				DataType:       bulkReq.DataType,
				OrderBookDepth: bulkReq.OrderBookDepth,
			}
			cm.requestCh <- singleReq
		}
		return
	}

	var req shared_types.ClientRequest
	json.Unmarshal(payload, &req)
	req.ClientID = clientID
	cm.requestCh <- &req
}