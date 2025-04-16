# Datenmigrationsreport
Erstellt am: 16.04.2025 09:39:43

## Zusammenfassung

### Migrationsstatistik

| Metrik | Wert |
|--------|------|
| Gesamtzahl Quelldatensätze | 1007 |
| Gesamtzahl Duplikate | 35 |
| Gesamtzahl migrierte Datensätze | 972 |
| Duplikatrate | 3.48% |
| Migrationsrate | 96.52% |

### Migrationsprozess-Übersicht

| Prozessschritt | Anzahl der Dateien |
|---------------|-------------------|
| Feldmappings | 1 |
| Wertmappings | 0 |
| Gemappte Dateien | 1 |
| Duplikat-Dateien | 1 |
| Eindeutige Datensätze | 1 |
| Finale Dateien | 1 |
| Zieldateien | 1 |
| Prozessberichte | 0 |

### Quelldaten

| Datei | Datensätze | Spalten | Dateigröße (KB) |
|-------|------------|---------|-----------------|
| MOCK_DATA | 1007 | 6 | 58.63 |

### Duplikate

| Datei | Schwellenwert | Anzahl Duplikate |
|-------|--------------|------------------|
| MOCK_DATA_mapped | 100% | 35 |

### Eindeutige Datensätze

| Datei | Anzahl eindeutiger Datensätze | % der Quelldaten |
|-------|------------------------------|------------------|
| MOCK_DATA_mapped | 972 | N/A |

### Zieldaten

| Datei | Datensätze | Spalten | Dateigröße (KB) |
|-------|------------|---------|-----------------|
| MOCK_DATA_target.csv | 28 | 6 | 1.58 |

### Finale Daten

| Datei | Behandlung | Datensätze | % der Quelldaten | Dateigröße (KB) |
|-------|------------|------------|------------------|-----------------|
| MOCK_DATA_mapped | standard | 972 | N/A | 56.71 |
