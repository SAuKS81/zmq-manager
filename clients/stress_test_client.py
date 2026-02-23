import zmq
import json
import time
import threading
import uuid
import random
import numpy as np
import ccxt

# ======================================================================
# --- STRESSTEST-KONFIGURATION ---
# ======================================================================
STRESS_TEST_CONFIG = [
    {
        "handler": "binance",
        "market_type": "spot",
        "data_type": "trades",
        "symbol_limit": 500, 
    },
    {
        "handler": "mexc",
        "market_type": "spot",
        "data_type": "trades",
        "symbol_limit": 500,
    },
    {
        "handler": "kucoin",
        "market_type": "spot",
        "data_type": "trades",
        "symbol_limit": 500,
    },
    {
        "handler": "bybit_native",
        "market_type": "swap",
        "data_type": "trades",
        "symbol_limit": 500,
    },
    # Fügen Sie hier Orderbuch-Tests hinzu, wenn gewünscht
    {
        "handler": "bybit_native",
        "market_type": "swap",
        "data_type": "orderbooks",
        "symbol_limit": 500, # Vorsicht, das sind viele Daten!
        "depth": 1,
    },
]
# ======================================================================


class BrokerClient:
    """
    Ein ZMQ-Client, der mehrere Datenströme (Trades, Orderbücher) vom Go-Broker
    empfangen, verarbeiten und statistisch auswerten kann.
    """
    def __init__(self, zmq_address="tcp://localhost:5555"):
        self.zmq_address = zmq_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        client_id = f"py-stresstest-{uuid.uuid4()}".encode('utf-8')
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
        
        self._ob_update_counter = 0
        self._total_ob_updates_processed = 0
        self._ob_latencies = []

    def _listen_for_messages(self):
        """Hört kontinuierlich auf eingehende Nachrichten und sortiert sie."""
        while not self._stop_event.is_set():
            try:
                if self.socket.poll(1000, zmq.POLLIN):
                    py_recv_ts = int(time.time() * 1000)
                    frames = self.socket.recv_multipart()
                    payload = frames[-1]
                    try:
                        data = json.loads(payload)
                        
                        # Ping/Pong-Handler (bleibt gleich)
                        if isinstance(data, dict) and data.get("type") == "ping":
                            self.send_pong()
                            continue

                        # +++ KORREKTUR: Einheitliche Verarbeitung für Listen +++
                        if isinstance(data, list) and len(data) > 0:
                            first_item = data[0]
                            data_type = first_item.get('data_type')

                            if data_type == 'trades':
                                with self._counter_lock:
                                    self._trade_counter += len(data)
                                    self._total_trades_processed += len(data)
                                    for trade in data:
                                        go_ts = trade.get('go_timestamp', 0)
                                        if go_ts > 0:
                                            self._internal_latencies.append(py_recv_ts - go_ts)
                            
                            elif data_type == 'orderbooks':
                                with self._counter_lock:
                                    self._ob_update_counter += len(data)
                                    self._total_ob_updates_processed += len(data)
                                    for ob in data:
                                        go_ts = ob.get('go_timestamp', 0)
                                        if go_ts > 0:
                                            self._ob_latencies.append(py_recv_ts - go_ts)

                    except (json.JSONDecodeError, IndexError, KeyError):
                        pass # Ignoriere fehlerhafte Nachrichten
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM: break
                else: print(f"Unerwarteter ZMQ-Fehler im Listener-Thread: {e}"); break
    
    def _log_stats(self):
        """Gibt alle 10 Sekunden eine aggregierte Statistik aus."""
        while not self._stop_event.is_set():
            if self._stop_event.wait(10): break
            
            with self._counter_lock:
                trade_count = self._trade_counter
                total_trades = self._total_trades_processed
                trade_lats = self._internal_latencies[:] # Kopie erstellen
                self._trade_counter = 0
                self._internal_latencies = []
                
                ob_count = self._ob_update_counter
                total_obs = self._total_ob_updates_processed
                ob_lats = self._ob_latencies[:] # Kopie erstellen
                self._ob_update_counter = 0
                self._ob_latencies = []

            print("\n" + "="*50)
            if total_trades > 0:
                rate = trade_count / 10.0
                print(f"[STATS|Trades]     Rate: {trade_count} in 10s ({rate:.2f}/s) | Gesamt: {total_trades}")
                if trade_lats:
                    arr = np.array(trade_lats)
                    print(f"                   └─ Latenz (ms): avg={np.mean(arr):.2f}, p95={np.percentile(arr, 95):.2f}, max={np.max(arr):.0f}")
            
            if total_obs > 0:
                rate = ob_count / 10.0
                print(f"[STATS|OrderBooks] Rate: {ob_count} in 10s ({rate:.2f}/s) | Gesamt: {total_obs}")
                if ob_lats:
                    arr = np.array(ob_lats)
                    print(f"                   └─ Latenz (ms): avg={np.mean(arr):.2f}, p95={np.percentile(arr, 95):.2f}, max={np.max(arr):.0f}")
            print("="*50)

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
        
    def send_pong(self):
        self.socket.send_json({"message": "pong"})
        
    def subscribe_bulk(self, exchange_handler: str, symbols: list, market_type: str, data_type: str, depth: int = 0):
        """Sendet eine Bulk-Anfrage an den Broker."""
        print(f"Sende Bulk-Subscribe an Handler '{exchange_handler}' für {len(symbols)} Symbole (Typ: {data_type}, Tiefe: {depth})...")
        command = {
            "action": "subscribe_bulk",
            "exchange": exchange_handler,
            "symbols": symbols,
            "market_type": market_type,
            "data_type": data_type
        }
        if data_type == 'orderbooks' and depth > 0:
            command['depth'] = depth
        try:
            self.socket.send_json(command)
        except Exception as e:
            print(f"[ERROR] Fehler beim Senden des Befehls: {e}")

def get_ccxt_symbols(exchange_id: str, market_type: str, limit: int):
    """Lädt eine Liste von aktiven USDT-Märkten für eine gegebene Börse."""
    print(f"Lade {limit} Märkte für {exchange_id.capitalize()} ({market_type})...")
    try:
        exchange = getattr(ccxt, exchange_id)({'options': {'defaultType': market_type}})
        exchange.load_markets()
    except (AttributeError, ccxt.ExchangeNotFound, ccxt.BaseError) as e:
        print(f"[ERROR] Konnte Märkte für '{exchange_id}' nicht laden: {e}")
        return []

    symbols = [
        m['symbol'] for m in exchange.markets.values()
        if m.get('active', True) and (
            (market_type == 'spot' and m.get('spot', False) and 'USDT' in m['symbol']) or
            (market_type == 'swap' and m.get('swap', False) and m['settle'] == 'USDT')
        )
    ]
    # Einfachere Filterung für Spot-Märkte, die USDT enthalten, da `quote` nicht immer `USDT` ist
    if market_type == 'spot':
        symbols = [s for s in symbols if '/USDT' in s]

    if not symbols:
        print(f"[WARN] Keine passenden USDT-{market_type}-Märkte für {exchange_id.capitalize()} gefunden.")
        return []
    
    random.shuffle(symbols)
    return symbols[:limit]

def main():
    """Hauptfunktion, die den Stresstest orchestriert."""
    print("Starte Stresstest-Client...")
    client = BrokerClient()
    client.start()
    
    total_subscriptions = 0
    
    for job in STRESS_TEST_CONFIG:
        handler = job.get("handler")
        market_type = job.get("market_type")
        data_type = job.get("data_type")
        symbol_limit = job.get("symbol_limit")
        depth = job.get("depth", 0)

        if not all([handler, market_type, data_type, symbol_limit]):
            print(f"[WARN] Überspringe ungültigen Job in der Konfiguration: {job}")
            continue

        print(f"\n--- Verarbeite Job: {handler.upper()} | {market_type.upper()} | {data_type.upper()} ---")
        
        exchange_name_for_symbols = handler.split('_')[0]
        
        symbols_to_sub = get_ccxt_symbols(exchange_name_for_symbols, market_type, symbol_limit)
        
        if symbols_to_sub:
            client.subscribe_bulk(handler, symbols_to_sub, market_type, data_type, depth)
            total_subscriptions += len(symbols_to_sub)
            time.sleep(0.5) 
        else:
            print(f"Keine Symbole für Job gefunden, überspringe.")

    print(f"\nAlle {len(STRESS_TEST_CONFIG)} Jobs verarbeitet. {total_subscriptions} Gesamt-Abonnements gesendet.")
    print("Lausche auf Daten... Drücke Ctrl+C zum Beenden.")
    
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