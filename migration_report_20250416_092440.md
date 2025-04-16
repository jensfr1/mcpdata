# Mig. 
Erstellt am: 16.04.2025 09:24:40

## Zusammenfassung

### Migrationsprozess-Übersicht

| Prozessschritt | Anzahl der Dateien |
|---------------|-------------------|
| Feldmappings | 1 |
| Wertmappings | 0 |
| Gemappte Dateien | 1 |
| Duplikat-Dateien | 1 |
| Finale Dateien | 1 |
| Prozessberichte | 0 |

### Quelldaten

| Datei | Datensätze | Spalten | Dateigröße (KB) |
|-------|------------|---------|-----------------|
| MOCK_DATA | 1007 | 6 | 58.63 |

### Duplikate

| Datei | Schwellenwert | Anzahl Duplikate |
|-------|--------------|------------------|
| MOCK_DATA_mapped | 100% | 35 |

#### Beispiele für Duplikate

**Duplikate in MOCK_DATA_mapped (Schwellenwert: 100%)**

| id | first_name | last_name | email | gender | ip_address | similarity_score |
|---|---|---|---|---|---|---|
| 1 | Ariadne | Jamison | ajamison0@discovery.com | F | 98.129.227.154 | 100.0 |
| 10 | Gustaf | Cabera | gcabera9@opera.com | M | 182.75.70.247 | 100.0 |
| 3 | Hyacinthe | Turfrey | hturfrey2@unicef.org | F | 148.89.58.69 | 100.0 |
| 4 | Nickola | Swate | nswate3@baidu.com | M | 93.24.165.30 | 100.0 |
| 5 | Korie | Makey | kmakey4@prweb.com | F | 161.86.101.16 | 100.0 |

### Finale Daten

| Datei | Behandlung | Datensätze | Dateigröße (KB) |
|-------|------------|------------|-----------------|
| MOCK_DATA_mapped | standard | 972 | 56.71 |
