# TEAM

## Rollen

- Stephan: Owner, PM, Operator (priorisiert und entscheidet Merge/No-Merge)
- Developer (Codex): Implementierung, Analyse, Commit/PR, Performance-Validierung

## Informationsfluss

- Stephan <-> Developer: direkte Abstimmung ueber Tasks, Commits und technische Entscheidungen
- Stephan fuehrt Vultr-Runs aus und liefert Logs/Metriken/pprof
- Developer liefert konkrete Run-Kommandos, wertet Ergebnisse aus und trifft pro Patch keep/revert-Empfehlung

## DoD fuer Performance-Arbeit

Ein Performance-Ticket gilt erst als erledigt, wenn alle Punkte vorliegen:

1. Commit-Hash ist gepinnt
2. Scripted Run wurde ausgefuehrt (`baseline_ingest.sh`)
3. Gate-Report ist dokumentiert (`PASS` oder `FAIL_DROPS_DETECTED`)
4. Vergleich gegen Referenz ist explizit bewertet (`keep` oder `revert`)
