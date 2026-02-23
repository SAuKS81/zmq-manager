import asyncio
import ccxt.pro as ccxtpro
import sys

# ======================================================================
# --- KONFIGURATION ---
# ======================================================================
# Verwende das CCXT Standardformat
SYMBOLS_TO_SUBSCRIBE = ['BTC/USDT']
MARKET_TYPE = 'swap'
# ======================================================================

async def main():
    """
    Verwendet die offizielle ccxt.pro Bibliothek, um den `orderbook.1` Stream von Bybit
    zu abonnieren und das Top-of-Book auszugeben.
    Dieses Skript dient als Referenzimplementierung.
    """
    print("--- Starte CCXT.pro Verifizierungs-Client ---")
    print(f"Börse: BYBIT | Markt-Typ: {MARKET_TYPE.upper()} | Tiefe: 1")
    print(f"Abonniere Symbole: {', '.join(SYMBOLS_TO_SUBSCRIBE)}")
    print("="*80)

    # Instanziiere die Bybit-Klasse von ccxt.pro
    exchange = ccxtpro.bybit({
        'options': {
            'defaultType': MARKET_TYPE,
        },
    })

    last_output = {}

    try:
        while True:
            # watch_order_book_for_symbols ist effizienter für mehrere Symbole
            # Der `limit=1` Parameter weist CCXT an, den `orderbook.1.SYMBOL`-Topic zu abonnieren.
            orderbook = await exchange.watch_order_book_for_symbols(SYMBOLS_TO_SUBSCRIBE, 1)

            # ----- Verarbeitung und Ausgabe (exakt wie in deinem Test-Client) -----
            
            symbol = orderbook.get('symbol')
            if not symbol:
                continue

            # WICHTIG: CCXT gibt leere Listen zurück, wenn eine Seite leer ist.
            # Wir müssen explizit darauf prüfen.
            bids = orderbook.get('bids', [])
            asks = orderbook.get('asks', [])

            best_bid = bids[0] if bids else None
            best_ask = asks[0] if asks else None

            display_symbol = symbol.replace('/', '-') + ':USDT'
            output = f" {display_symbol:<15} | "

            if best_bid:
                price, amount = best_bid
                output += f"BID: {price:10.4f} ({amount:8.4f}) | "
            else:
                # Dieser Fall wird eintreten und ist der Beweis.
                output += f"BID: {'N/A':<22} | "

            if best_ask:
                price, amount = best_ask
                output += f"ASK: {price:10.4f} ({amount:8.4f})"
            else:
                # Dieser Fall wird eintreten und ist der Beweis.
                output += f"ASK: {'N/A':<22}"

            if last_output.get(symbol) != output:
                print(output)
                last_output[symbol] = output

    except (asyncio.CancelledError, KeyboardInterrupt):
        print("\nClient wird beendet...")
    except Exception as e:
        print(f"\nEin Fehler ist aufgetreten: {e}")
        # Zeige den Traceback für detaillierte Fehleranalyse an
        import traceback
        traceback.print_exc()
    finally:
        print("Schließe Exchange-Verbindung...")
        await exchange.close()
        print("Verbindung geschlossen.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)