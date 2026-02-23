import zmq
import json
import time
import threading
import uuid
import random
import numpy as np
import ccxt

# ======================================================================
# --- KONFIGURATION ---
# ======================================================================
# NEU: Wähle hier den exakten HANDLER-Namen, den der Server verwenden soll.
#
# Beispiele:
# - 'binance_native' -> Verwendet den nativen Go-Handler für Binance (nur Trades).
# - 'bybit_native'   -> Verwendet den nativen Go-Handler für Bybit (nur Trades).
# - 'binance'        -> Kein nativer Treffer, Server nutzt den CCXT-Handler für Binance.
# - 'kucoin'         -> Kein nativer Treffer, Server nutzt den CCXT-Handler für Kucoin.
#
HANDLER_TO_TEST = 'bybit'

# Der Markt-Typ bleibt wie bisher.
MARKET_TYPE_TO_TEST = 'swap'

# Wähle den Datentyp: "trades" oder "orderbooks".
# (Beachte: "orderbooks" funktioniert nur, wenn der Handler es unterstützt, also CCXT).
DATA_TYPE_TO_TEST = 'trades' 

SYMBOL_LIMIT = 50
ORDERBOOK_DEPTH = 5
# ======================================================================

# ... (Die BrokerClient Klasse bleibt exakt so, wie ich sie dir zuletzt geschickt habe) ...
class BrokerClient:
    def __init__(self, zmq_address="tcp://localhost:5555"):
        self.zmq_address = zmq_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        client_id = f"py-client-{uuid.uuid4()}".encode('utf-8')
        self.socket.setsockopt(zmq.IDENTITY, client_id)
        self.socket.connect(self.zmq_address)
        print(f"Client mit ID '{client_id.decode()}' verbunden mit {self.zmq_address}")
        
        self._stop_event = threading.Event()
        self._listener_thread = threading.Thread(target=self._listen_for_messages, daemon=True)
        self._stats_thread = threading.Thread(target=self._log_stats, daemon=True)
        
        self.subscriptions = {}
        self._counter_lock = threading.Lock()
        
        self._trade_counter = 0
        self._total_trades_processed = 0
        self._internal_latencies = []
        self._e2e_latencies = [] 
        
        self._ob_update_counter = 0
        self._total_ob_updates_processed = 0
        self._ob_latencies = []

    def _listen_for_messages(self):
        while not self._stop_event.is_set():
            try:
                if self.socket.poll(1000, zmq.POLLIN):
                    py_recv_ts = int(time.time() * 1000)
                    frames = self.socket.recv_multipart()
                    payload = frames[-1]
                    try:
                        data = json.loads(payload)
                        
                        if isinstance(data, dict) and data.get("type") == "ping":
                            self.send_pong()
                            continue

                        if isinstance(data, list) and len(data) > 0 and data[0].get('data_type') == 'trades':
                            with self._counter_lock:
                                self._trade_counter += len(data)
                                self._total_trades_processed += len(data)
                                for trade in data:
                                    bybit_ts = trade.get('timestamp', 0)
                                    go_ts = trade.get('go_timestamp', 0)
                                    if bybit_ts > 0 and go_ts > 0:
                                        e2e_latency = py_recv_ts - bybit_ts
                                        internal_latency = py_recv_ts - go_ts
                                        if 0 < internal_latency < 20000:
                                            self._internal_latencies.append(internal_latency)
                                            self._e2e_latencies.append(e2e_latency)

                        elif isinstance(data, dict) and data.get('data_type') == 'orderbooks':
                             with self._counter_lock:
                                self._ob_update_counter += 1
                                self._total_ob_updates_processed += 1
                                go_ts = data.get('go_timestamp', 0)
                                if go_ts > 0:
                                    latency = py_recv_ts - go_ts
                                    if 0 < latency < 20000:
                                        self._ob_latencies.append(latency)

                    except (json.JSONDecodeError, IndexError, KeyError):
                        pass
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM: break
                else: print(f"Unerwarteter ZMQ-Fehler im Listener-Thread: {e}"); break
    
    def _log_stats(self):
        while not self._stop_event.is_set():
            if self._stop_event.wait(10): break
            
            with self._counter_lock:
                trade_count_in_window = self._trade_counter
                total_trade_count = self._total_trades_processed
                internal_lats = self._internal_latencies
                e2e_lats = self._e2e_latencies
                self._trade_counter = 0
                self._internal_latencies = []
                self._e2e_latencies = []
                
                ob_count_in_window = self._ob_update_counter
                total_ob_count = self._total_ob_updates_processed
                ob_lats = self._ob_latencies
                self._ob_update_counter = 0
                self._ob_latencies = []

            if total_trade_count > 0:
                trades_per_sec = trade_count_in_window / 10.0
                print(f"[STATS|Trades] Rate: {trade_count_in_window} in 10s ({trades_per_sec:.2f}/s) | Gesamt: {total_trade_count}")
                if internal_lats:
                    lat_array = np.array(internal_lats)
                    print(f"         └─ Internal Latency (ms): avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, max={np.max(lat_array):.0f}")
                if e2e_lats:
                    lat_array = np.array(e2e_lats)
                    print(f"         └─ E2E Latency      (ms): avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, max={np.max(lat_array):.0f}")
            
            if total_ob_count > 0:
                ob_updates_per_sec = ob_count_in_window / 10.0
                print(f"[STATS|OrderBooks] Rate: {ob_count_in_window} in 10s ({ob_updates_per_sec:.2f}/s) | Gesamt: {total_ob_count}")
                if ob_lats:
                    lat_array = np.array(ob_lats)
                    print(f"               └─ Latency (ms): avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, max={np.max(lat_array):.0f}")


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
        
    def subscribe_bulk(self, exchange_handler: str, symbols: list, market_type: str, data_type: str, depth: int = 0):
        print(f"Sende Bulk-Subscribe an Handler '{exchange_handler}' für {len(symbols)} Symbole (Typ: {data_type}, Tiefe: {depth})...")
        command = {
            "action": "subscribe_bulk",
            "exchange": exchange_handler, # Wichtig: Wir senden den vollständigen Handler-Namen
            "symbols": symbols,
            "market_type": market_type,
            "data_type": data_type
        }
        if data_type == 'orderbooks' and depth > 0:
            command['depth'] = depth
        self.send_command(command)
        for symbol in symbols:
            sub_key = f"{exchange_handler}-{symbol}-{market_type}-{data_type}"
            self.subscriptions[sub_key] = True

# ... (get_ccxt_symbols bleibt identisch) ...
def get_ccxt_symbols(exchange_id: str, market_type: str, limit: int):
    print(f"Lade Märkte für {exchange_id.capitalize()}...")
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
    print(f"Starte Test für Handler: {HANDLER_TO_TEST.upper()} / {MARKET_TYPE_TO_TEST.upper()} / {DATA_TYPE_TO_TEST.upper()}")
    
    # Wir brauchen den reinen Börsennamen, um die Symbole über CCXT zu laden.
    # z.B. aus 'binance_native' wird 'binance'.
    exchange_name_for_symbols = HANDLER_TO_TEST.split('_')[0]
    
    symbols_to_sub = get_ccxt_symbols(exchange_name_for_symbols, MARKET_TYPE_TO_TEST, limit=SYMBOL_LIMIT)
    if not symbols_to_sub:
        return
        
    print(f"{len(symbols_to_sub)} Symbole zum Abonnieren gefunden. Starte Client...")
    client = BrokerClient()
    client.start()
    
    # Wir übergeben jetzt den vollständigen, konfigurierten Handler-Namen.
    client.subscribe_bulk(
        HANDLER_TO_TEST, 
        symbols_to_sub, 
        MARKET_TYPE_TO_TEST, 
        DATA_TYPE_TO_TEST,
        depth=ORDERBOOK_DEPTH
    )
    
    print(f"Bulk-Anfrage an Handler '{HANDLER_TO_TEST}' gesendet. Lausche auf Daten...")
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
    except ImportError as e:
        print(f"Fehlende Bibliothek: {e.name}. Bitte installieren: pip install numpy ccxt")
    else:
        main()