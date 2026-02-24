# TEAM

## Rollen

- Stephan: Owner, Operator, Messenger
- PM/Architect/Observer: Prioritaeten, Gates, Review-Entscheidungen
- Developer: Implementierung im Repo, Tasks, Commits/PRs

## Informationsfluss

- PM/Architect <-> Developer: direkt ueber Tasks, Review und technische Entscheidungen
- Stephan <-> PM/Developer: Ausfuehrung von Commands auf Zielsystem, Rueckgabe von Logs/Metriken/pprof
- Stephan fungiert als Messenger/Executor fuer Laufdaten aus Vultr

## DoD fuer Performance-Arbeit

Ein Performance-Ticket gilt erst als erledigt, wenn alle Punkte vorliegen:

1. Commit-Hash ist gepinnt
2. Scripted Run wurde ausgefuehrt (`baseline_ingest.sh`)
3. Gate-Report ist dokumentiert (`PASS` oder `FAIL_DROPS_DETECTED`)
