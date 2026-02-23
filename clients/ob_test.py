import zmq
import json
import time
import threading
import uuid
import sys

# NEU: MessagePack für High-Speed Decoding
try:
    import msgpack
except ImportError:
    print("Fehler: Das Modul 'msgpack' fehlt.")
    print("Bitte installiere es mit: pip install msgpack")
    sys.exit(1)

# ======================================================================
# --- KONFIGURATION ---
# ======================================================================
HANDLER = 'binance_native' 
MARKET_TYPE = 'spot'
SYMBOLS_TO_SUBSCRIBE = ['BTC/USDT', 'ETH/USDT'] 
ORDERBOOK_DEPTH = 5
ZMQ_ADDRESS = "tcp://localhost:5555"
# ======================================================================

class BrokerClient:
    def __init__(self, zmq_address=ZMQ_ADDRESS):
        self.zmq_address = zmq_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        
        # Eindeutige ID setzen
        client_id = f"py-ob-{uuid.uuid4().hex[:8]}".encode('utf-8')
        self.socket.setsockopt(zmq.IDENTITY, client_id)
        
        self._stop_event = threading.Event()
        self._listener_thread = threading.Thread(target=self._listen_for_messages, daemon=True)
        
        self._output_lock = threading.Lock()
        self.last_output = {}

    def start(self):
        try:
            self.socket.connect(self.zmq_address)
            print(f"Client '{self.socket.getsockopt(zmq.IDENTITY).decode()}' verbunden mit {self.zmq_address}")
            self._listener_thread.start()
            return True
        except zmq.ZMQError as e:
            print(f"Fehler bei der Verbindung zum Broker: {e}")
            return False

    def stop(self):
        if not self._stop_event.is_set():
            print("\nStoppe Client...")
            self._stop_event.set()
            self._listener_thread.join(timeout=2)
            if not self.socket.closed: self.socket.close()
            if not self.context.closed: self.context.term()
            print("Client gestoppt.")

    def _process_managed_book(self, book_data: dict):
        """Verarbeitet das Orderbuch (jetzt aus MsgPack dict)."""
        # MsgPack Keys sind oft Bytes oder verkürzt (z.B. 's' statt 'symbol')
        # Wir haben im Go-Code Tags definiert:
        # Symbol='s', Bids='b', Asks='a', Price='p', Amount='a'
        
        # Mapping auf die Kurzform der Keys aus Go (msgpack tags)
        symbol = book_data.get('s') or book_data.get('symbol')
        if not symbol: return
            
        bids = book_data.get('b') or book_data.get('bids') or []
        asks = book_data.get('a') or book_data.get('asks') or []

        # Helper um Price/Amount sicher zu lesen (MsgPack liefert floats)
        def get_level(levels, idx):
            if idx < len(levels):
                lvl = levels[idx]
                # MsgPack struct mapping: p=price, a=amount
                return lvl.get('p'), lvl.get('a')
            return 'N/A', 'N/A'

        bid_p, bid_a = get_level(bids, 0)
        ask_p, ask_a = get_level(asks, 0)

        display_symbol = symbol.replace('/', '-')
        output = f" {display_symbol:<15} | "
        
        if isinstance(bid_p, (int, float)):
            output += f"BID: {bid_p:10.4f} ({bid_a:8.4f}) | "
        else:
            output += f"BID: {'N/A':<22} | "
            
        if isinstance(ask_p, (int, float)):
            output += f"ASK: {ask_p:10.4f} ({ask_a:8.4f})"
        else:
            output += f"ASK: {'N/A':<22}"

        with self._output_lock:
            if self.last_output.get(symbol) != output:
                print(output)
                self.last_output[symbol] = output

    def _listen_for_messages(self):
        """Lauscht auf Nachrichten (MsgPack oder JSON Ping)."""
        while not self._stop_event.is_set():
            try:
                if self.socket.poll(1000, zmq.POLLIN):
                    # DEALER empfängt Multipart: [Header, Payload]
                    msg_parts = self.socket.recv_multipart()
                    
                    if len(msg_parts) < 2:
                        continue
                        
                    header = msg_parts[-2] # Vorletztes Frame ist Typ/Header
                    payload = msg_parts[-1] # Letztes Frame sind Daten

                    # 1. Fall: Binäre Daten (MsgPack) - Header "O" (Orderbook) oder "T" (Trade)
                    if header in [b'O', b'T']:
                        try:
                            # raw=False damit Strings als Strings kommen, nicht als Bytes
                            data = msgpack.unpackb(payload, raw=False)
                            
                            # Go sendet oft Listen (Batches)
                            if isinstance(data, list):
                                for item in data:
                                    if header == b'O':
                                        self._process_managed_book(item)
                                    # Trade logic hier einfügen wenn nötig
                            elif isinstance(data, dict):
                                if header == b'O':
                                    self._process_managed_book(data)
                                    
                        except Exception as e:
                            print(f"MsgPack Decode Error: {e}")

                    # 2. Fall: Leerer Header oder JSON (Ping kommt oft so)
                    else:
                        try:
                            data = json.loads(payload)
                            if isinstance(data, dict) and data.get("type") == "ping":
                                self.send_pong()
                        except:
                            pass # Ignoriere nicht-JSON Müll
                            
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM: break
                print(f"ZMQ-Fehler im Listener: {e}")
                break

    def send_pong(self):
        # Pongs senden wir weiterhin als JSON, das erwartet der Go-Broker so
        self.socket.send_json({"message": "pong"})

    def subscribe_bulk(self, handler: str, symbols: list, market_type: str, data_type: str, depth: int):
        print(f"\nSende Bulk-Subscribe an Handler '{handler}' für {len(symbols)} Symbole...")
        
        # WICHTIG: encoding: msgpack anfordern!
        command = {
            "action": "subscribe_bulk",
            "encoding": "msgpack",     # <--- HIER SAGEN WIR DEM BROKER WAS WIR WOLLEN
            "exchange": handler,
            "symbols": symbols,
            "market_type": market_type,
            "data_type": data_type,
            "depth": depth
        }
        # Requests senden wir als JSON (Broker Input ist immer JSON)
        self.socket.send_json(command)

def main():
    print("--- Starte Bybit/Binance Native Client (MsgPack Mode) ---")
    print(f"Handler: {HANDLER.upper()} | Markt-Typ: {MARKET_TYPE.upper()} | Tiefe: {ORDERBOOK_DEPTH}")
    
    client = BrokerClient()
    if not client.start():
        return

    client.subscribe_bulk(
        handler=HANDLER, 
        symbols=SYMBOLS_TO_SUBSCRIBE, 
        market_type=MARKET_TYPE,
        data_type='orderbooks',
        depth=ORDERBOOK_DEPTH
    )
    
    print("\nAbonnement-Anfrage gesendet (MsgPack). Warte auf Daten...")
    print("="*80)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()

if __name__ == "__main__":
    main()