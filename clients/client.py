import zmq
import json
import time
import threading
import uuid
import requests
import random
import numpy as np

class BrokerClient:
    def __init__(self, zmq_address="tcp://localhost:5555"):
        # ... (init bleibt größtenteils gleich)
        self.zmq_address = zmq_address
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        client_id = f"py-client-{uuid.uuid4()}".encode('utf-8')
        self.socket.setsockopt(zmq.IDENTITY, client_id)
        self.socket.connect(self.zmq_address)
        print(f"Client mit ID '{client_id.decode()}' verbunden mit {self.zmq_address}")
        self._stop_event = threading.Event()
        self._listener_thread = threading.Thread(target=self._listen_for_messages, daemon=True)
        self.subscriptions = {}
        
        self._counter_lock = threading.Lock()
        self._trade_counter = 0
        self._total_trades_processed = 0
        
        # NEU: Wir sammeln jetzt beide Latenzen
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
                                bybit_ts = trade.get('timestamp', 0)
                                go_ts = trade.get('go_timestamp', 0)
                                
                                if bybit_ts > 0 and go_ts > 0:
                                    e2e_latency = py_recv_ts - bybit_ts
                                    internal_latency = py_recv_ts - go_ts
                                    
                                    # Ignoriere negative interne Latenz (Zeitsprünge), aber erlaube negative E2E (Uhrendrift)
                                    if 0 < internal_latency < 20000:
                                        with self._counter_lock:
                                            self._internal_latencies.append(internal_latency)
                                            self._e2e_latencies.append(e2e_latency) # NEU: E2E-Latenz auch hinzufügen
                                
                                with self._counter_lock:
                                    self._trade_counter += 1
                                    self._total_trades_processed += 1
                        elif isinstance(trade_batch, dict) and trade_batch.get("type") == "ping":
                            self.send_pong()
                    except json.JSONDecodeError:
                        pass
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM: break
                else: print(f"Unerwarteter ZMQ-Fehler im Listener-Thread: {e}"); break
    
    def _log_stats(self):
        while not self._stop_event.is_set():
            if self._stop_event.wait(10):
                break
            
            with self._counter_lock:
                count_in_window = self._trade_counter
                total_count = self._total_trades_processed
                internal_lats = self._internal_latencies
                e2e_lats = self._e2e_latencies # NEU
                
                self._trade_counter = 0
                self._internal_latencies = []
                self._e2e_latencies = [] # NEU

            trades_per_sec = count_in_window / 10.0
            
            print(f"[STATS] Rate: {count_in_window} in 10s ({trades_per_sec:.2f}/s) | Gesamt: {total_count}")

            # Statistische Auswertung für Internal Latency
            if internal_lats:
                lat_array = np.array(internal_lats)
                print(f"        └─ Internal Latency (ms): min={np.min(lat_array):.0f}, avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, p99={np.percentile(lat_array, 99):.2f}, max={np.max(lat_array):.0f}")
            
            # NEU: Statistische Auswertung für End-to-End Latency
            if e2e_lats:
                lat_array = np.array(e2e_lats)
                print(f"        └─ E2E Latency      (ms): min={np.min(lat_array):.0f}, avg={np.mean(lat_array):.2f}, p95={np.percentile(lat_array, 95):.2f}, p99={np.percentile(lat_array, 99):.2f}, max={np.max(lat_array):.0f}")

    # ... (Rest der Klasse und der Datei bleibt unverändert) ...
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
    def subscribe(self, exchange: str, symbol: str, market_type: str):
        sub_key = f"{exchange}-{symbol}-{market_type}"
        if sub_key in self.subscriptions: return
        command = {"action": "subscribe", "exchange": exchange, "symbol": symbol, "market_type": market_type}
        self.send_command(command)
        self.subscriptions[sub_key] = command

def get_bybit_swap_symbols(limit=500):
    url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        symbols = []
        for item in data.get("result", {}).get("list", []):
            if "USDT" in item.get("symbol", "") and not "USDC" in item.get("symbol", ""):
                base = item["symbol"].replace("USDT", "")
                ccxt_symbol = f"{base}/USDT:USDT"
                symbols.append(ccxt_symbol)
        if not symbols: return []
        random.shuffle(symbols)
        return symbols[:limit]
    except requests.RequestException as e:
        print(f"[ERROR] Fehler beim Abrufen der Bybit-Symbole: {e}")
        return []
        
def main_bulk_subscribe():
    print("Starte Massen-Abonnement-Test...")
    symbols_to_sub = get_bybit_swap_symbols(500)
    if not symbols_to_sub: return
    print(f"{len(symbols_to_sub)} Symbole zum Abonnieren gefunden. Starte Client...")
    client = BrokerClient()
    client.start()
    print("Sende Subscribe-Anfragen...")
    for symbol in symbols_to_sub:
        client.subscribe("bybit", symbol, "swap")
        time.sleep(0.01)
    print(f"{len(symbols_to_sub)} Subscribe-Anfragen gesendet. Lausche auf Trades...")
    print("Drücke Ctrl+C zum Beenden.")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt: pass
    finally: client.stop()

if __name__ == "__main__":
    try:
        import numpy
    except ImportError:
        print("Die 'numpy' Bibliothek wird benötigt. Bitte installieren mit: pip install numpy")
    else:
        main_bulk_subscribe()