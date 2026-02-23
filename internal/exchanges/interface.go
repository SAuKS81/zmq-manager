package exchanges

import "bybit-watcher/internal/shared_types"

// Exchange ist das neue, übergeordnete Interface für eine Börsenimplementierung.
type Exchange interface {
	// HandleRequest verarbeitet eine Abonnement-Anfrage für diese Börse.
	HandleRequest(req *shared_types.ClientRequest) // KORREKTUR
	// Stop beendet alle Aktivitäten für diese Börse.
	Stop()
}
