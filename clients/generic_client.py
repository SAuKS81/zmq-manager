import zmq
import json
import time
import threading
import uuid
import random
import numpy as np
import ccxt

# ======================================================================
# --- KONFIGURATION FÜR DEN BITGET-CLIENT ---
# ======================================================================
# Feste Börse für diesen Client
EXCHANGE_ID = 'binance' 
# Der zu testende Markt-Typ ('spot' oder 'swap')
MARKET_TYPE_TO_TEST = 'swap'
# Anzahl der Symbole, die abonniert werden sollen. 
# 50 ist ein guter Wert, da ein Bitget-Shard im Go-Code auf 50 Symbole ausgelegt ist.
SYMBOL_LIMIT = 1000
# ======================================================================

class BrokerClient:
    """
    Diese Klasse verwaltet die ZMQ-Verbindung zum Go-Broker, sendet Anfragen
    und verarbeitet eingehende Trade-Daten sowie Statistiken.
    Die Klasse ist identisch mit dem generischen CCXT-Client.
    """
    def __init__(self, zmq_address="tcp://localhost:5555"):
        self.zmq_address = zmq_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        client_id = f"py-bitget-client-{uuid.uuid4()}".encode('utf-8')
        self.socket.setsockopt(zmq.IDENTITY, client_id)
        self.socket.connect(self.zmq_address)
        print(f"Client mit ID '{client_id.decode()}' verbunden mit {self.zmq_address}")
        self._stop_event = threading.Event()
        self._listener_thread = threading.Thread(target=self._listen_for_messages, daemon=True)
        self.subscriptions = {}
        self._counter_lock = threading.Lock()
        self._trade_counter = 0
        self._total_trades_processed = 0
        self._internal_latencies = []
        self._e2e_latencies = [] 
        self._stats_thread = threading.Thread(target=self._log_stats, daemon=True)

    def _listen_for_messages(self):
        while not self._stop_event.is_set():
            try:
                if self.socket.poll(1000, zmq.POLLIN):
                    py_recv_ts = int(time.time() * 1000)
                    frames = self.socket.recv_multipart()
                    payload = frames[-1]
                    try:
                        trade_batch = json.loads(payload)
                        if isinstance(trade_batch, list):
                            for trade in trade_batch:
                                exchange_ts = trade.get('timestamp', 0)
                                go_ts = trade.get('go_timestamp', 0)
                                if exchange_ts > 0 and go_ts > 0:
                                    e2e_latency = py_recv_ts - exchange_ts
                                    internal_latency = py_recv_ts - go_ts
                                    # Nur plausible Latenzen berücksichtigen
                                    if 0 < internal_latency < 20000:
                                        with self._counter_lock:
                                            self._internal_latencies.append(internal_latency)
                                            self._e2e_latencies.append(e2e_latency)
                                with self._counter_lock:
                                    self._trade_counter += 1
                                    self._total_trades_processed += 1
                        elif isinstance(trade_batch, dict) and trade_batch.get("type") == "ping":
                            self.send_pong()
                    except json.JSONDecodeError:
                        # Ignoriere Nachrichten, die kein gültiges JSON sind
                        pass
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM: break
                else: print(f"Unerwarteter ZMQ-Fehler im Listener-Thread: {e}"); break
    
    def _log_stats(self):
        while not self._stop_event.is_set():
            if self._stop_event.wait(10): break
            with self._counter_lock:
                count_in_window = self._trade_counter
                total_count = self._total_trades_processed
                internal_lats = self._internal_latencies
                e2e_lats = self._e2e_latencies
                self._trade_counter = 0
                self._internal_latencies = []
                self._e2e_latencies = []
            trades_per_sec = count_in_window / 10.0
            print(f"[STATS] Rate: {count_in_window} in 10s ({trades_per_sec:.2f}/s) | Gesamt: {total_count}")
            if internal_lats:
                lat_array = np.array(internal_lats)
                print(f"        └─ Internal Latency (ms): min={np.min(lat_array):.0f}, avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, p99={np.percentile(lat_array, 99):.2f}, max={np.max(lat_array):.0f}")
            if e2e_lats:
                lat_array = np.array(e2e_lats)
                print(f"        └─ E2E Latency      (ms): min={np.min(lat_array):.0f}, avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, p99={np.percentile(lat_array, 99):.2f}, max={np.max(lat_array):.0f}")

    def start(self):
        self._listener_thread.start()
        self._stats_thread.start()
        
    def stop(self):
        print("\nStoppe Client...")
        self._stop_event.set()
        self._listener_thread.join(timeout=2)
        self._stats_thread.join(timeout=2)
        if not self.socket.closed: self.socket.close()
        if not self.context.closed: self.context.term()
        print("Client gestoppt.")
        
    def send_command(self, command: dict):
        try: self.socket.send_json(command)
        except Exception as e: print(f"[ERROR] Fehler beim Senden des Befehls: {e}")
        
    def send_pong(self):
        self.socket.send_json({"message": "pong"})
        
    def subscribe_bulk(self, exchange: str, symbols: list, market_type: str):
        """Sendet eine einzelne Anfrage für eine Liste von Symbolen."""
        print(f"Sende Bulk-Subscribe-Anfrage für {len(symbols)} Symbole an den Broker...")
        command = {
            "action": "subscribe_bulk",
            "exchange": exchange,
            "symbols": symbols,
            "market_type": market_type
        }
        self.send_command(command)
        # Intern merken, was wir abonniert haben
        for symbol in symbols:
            sub_key = f"{exchange}-{symbol}-{market_type}"
            self.subscriptions[sub_key] = True


def get_ccxt_symbols(exchange_id: str, market_type: str, limit: int):
    """
    Lädt verfügbare und aktive USDT-basierte Symbole für die angegebene
    Börse und den Markt-Typ mit der CCXT-Bibliothek.
    """
    print(f"Lade Märkte für {exchange_id.capitalize()} über CCXT...")
    try:
        options = {'options': {'defaultType': market_type}}
        exchange = getattr(ccxt, exchange_id)(options)
        exchange.load_markets()
    except (AttributeError, ccxt.ExchangeNotFound):
        print(f"[ERROR] Die Börse '{exchange_id}' wird von CCXT nicht gefunden oder unterstützt.")
        return []
    except ccxt.BaseError as e:
        print(f"[ERROR] CCXT-Fehler beim Laden der Märkte für {exchange_id}: {e}")
        return []

    symbols = []
    for market in exchange.markets.values():
        if not market.get('active', True):
            continue

        is_spot = market.get('spot', False)
        is_swap = market.get('swap', False)
        is_usdt_settled = market.get('quote') == 'USDT' or market.get('settle') == 'USDT'

        if (market_type == 'spot' and is_spot and is_usdt_settled) or \
           (market_type == 'swap' and is_swap and is_usdt_settled):
            symbols.append(market['symbol'])

    if not symbols:
        print(f"[WARN] Keine passenden {market_type}-Märkte für {exchange_id.capitalize()} gefunden.")
        return []
    
    random.shuffle(symbols)
    return symbols[:limit]


def main():
    """
    Hauptfunktion: Initialisiert den Client und startet den Test
    spezifisch für die Bitget-Schnittstelle.
    """
    print(f"--- Starte Bitget Test-Client ---")
    print(f"Börse: {EXCHANGE_ID.upper()} | Markt-Typ: {MARKET_TYPE_TO_TEST.upper()}")
    
    symbols_to_sub = get_ccxt_symbols(EXCHANGE_ID, MARKET_TYPE_TO_TEST, limit=SYMBOL_LIMIT)
    if not symbols_to_sub:
        print("Keine Symbole zum Abonnieren gefunden. Client wird beendet.")
        return
        
    print(f"{len(symbols_to_sub)} Symbole zum Abonnieren gefunden. Starte Broker-Client...")

    client = BrokerClient()
    client.start()
    
    # Sende eine einzelne Bulk-Anfrage für alle gefundenen Bitget-Symbole
    client.subscribe_bulk(EXCHANGE_ID, symbols_to_sub, MARKET_TYPE_TO_TEST)
    
    print("\nBulk-Anfrage gesendet. Lausche auf Trades...")
    print("Statistiken werden alle 10 Sekunden ausgegeben.")
    print("Drücke Ctrl+C zum Beenden.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.stop()

if __name__ == "__main__":
    try:
        import numpy
        import ccxt
        import zmq
    except ImportError as e:
        print(f"Fehlende Bibliothek: {e.name}. Bitte installieren: pip install numpy ccxt pyzmq")
    else:
        main()