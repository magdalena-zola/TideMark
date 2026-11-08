# TideMark / StableCQ — pełny lokalny pipeline ICDE v0.2.5

Pakiet jest przeznaczony do uruchomienia **lokalnie na pełnym pliku AIS**, bez przesyłania surowych danych do ChatGPT. Pipeline jest etapowy i wznawialny. Surowy CSV oraz ciężkie pliki Parquet zostają na komputerze; po zakończeniu należy przekazać wyłącznie `TideMark_RETURN.zip`.

## Co liczy pipeline

1. **Ingest** — pojedynczy odczyt CSV i konwersja do typowanego, skompresowanego Parquet. Zachowywany jest `export_row_id`, aby nie zgubić proweniencji fizycznego eksportu.
2. **Audit** — zakresy czasu, opóźnienia `ProcessedDate - MovementDateTime`, błędy czasu/pozycji/MMSI, profil dzienny, przybliżoną diagnostykę `MovementID` (dokładne grupowanie jest opcjonalne), bloki eksportu i automatyczny wybór najdłuższego spójnego okresu eksperymentalnego.
3. **Snapshots** — 15-minutowe okna i siatka 0.25°; trzy zapytania: liczba komunikatów, liczba różnych MMSI i liczba różnych szybkich jednostek (domyślnie `speed >= 12 kn`). Dla każdego okna wyliczane są odpowiedzi przy serii opóźnień finalizacji do 24 h. Migawki są zapisywane szeroko (jedna linia na okno i zapytanie), aby nie mnożyć danych przez liczbę opóźnień.
4. **Chronologiczny split** — train/calibration/test, bez losowego mieszania rekordów między czasem treningowym i testowym.
5. **Baselines i TideMark** — globalna stała zwłoka, stała zwłoka zależna od zapytania, kontekstowy TideMark (`query × hour × activity`, z hierarchicznym fallbackiem), plus offline oracle wyłącznie jako dolna granica.
6. **Ocena** — błąd, violation rate, selected delay, exactness, bootstrap po dniach, tabele polityk i krzywe delay–error.
7. **Raport** — PNG, Markdown i mały `TideMark_RETURN.zip` do dalszej analizy i przygotowania artykułu.

`ProcessedDate` jest traktowany jako **proxy dostępności rekordu w historycznej bazie**, a nie jako zweryfikowany czas odbioru z sieci. Pipeline nie zamienia tego założenia w fakt.

## Instalacja (Windows PowerShell)

Wymagany Python 3.11+ (zalecane 3.12/3.13) i 64-bitowy system.

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## 1. Najpierw smoke test na danych syntetycznych

```powershell
python selftest.py
```

Jeżeli zobaczysz `SELFTEST OK`, środowisko jest poprawne.

## 2. Krótki test na prawdziwym CSV

Użyj nowego katalogu wynikowego:

```powershell
python run_pipeline.py "D:\AIS\ihs_baltic.csv" "D:\AIS\TideMark_smoke" --preset standard --quick-rows 2000000
```

`--quick-rows` oznacza fizyczny prefiks CSV i służy tylko do kontroli technicznej. **Nie używamy jego wyników jako wyników publikacyjnych.**

## 3. Pełne obliczenie

```powershell
python run_pipeline.py "D:\AIS\ihs_baltic.csv" "D:\AIS\TideMark_full" --preset standard
```

Profile zasobów:

- `--preset low` → DuckDB 4 GB, 2 wątki,
- `--preset standard` → 8 GB, 4 wątki,
- `--preset high` → 24 GB, 8 wątków.

Możesz nadpisać ustawienia, np.:

```powershell
python run_pipeline.py "D:\AIS\ihs_baltic.csv" "D:\AIS\TideMark_full" --memory-limit 16GB --threads 8
```

Nie ustawiaj DuckDB na prawie cały RAM systemu. Katalog `spill` jest używany do operacji przekraczających limit pamięci. Dobrze, aby katalog wynikowy był na szybkim SSD/NVMe z dużym zapasem wolnego miejsca.

## Wznawianie

Po udanym etapie powstaje marker w `_stages/`. Ponowne uruchomienie tej samej komendy pomija gotowe etapy.

Jeżeli chcesz przeliczyć etap i wszystkie kolejne po zmianie kodu/konfiguracji:

```powershell
python run_pipeline.py "D:\AIS\ihs_baltic.csv" "D:\AIS\TideMark_full" --restart-stage snapshots
```

Uwaga: `--restart-stage ingest` wymaga ręcznego usunięcia starego `core_parquet`, aby nie nadpisać częściowo istniejącego korpusu bez kontroli.

Można również zatrzymać obliczenia po etapie:

```powershell
python run_pipeline.py "D:\AIS\ihs_baltic.csv" "D:\AIS\TideMark_full" --stop-after audit
```

## Co odesłać po obliczeniach

Tylko:

```text
D:\AIS\TideMark_full\TideMark_RETURN.zip
```

Nie wysyłaj `core_parquet/`, `snapshots/` ani `spill/`. ZIP zwrotny zawiera audyt, podziały czasowe, krzywe, reguły TideMark, bootstrap, rysunki, manifesty i logi.

## Ważne ograniczenia metodologiczne

- Surowy porządek CSV nie jest utożsamiany z kolejnością odbioru komunikatów.
- Ujemne `ProcessedDate - MovementDateTime` są traktowane jako anomalia i wykluczane z eksperymentu stabilności, a nie przycinane do zera.
- MMSI jest identyfikatorem raportowanym, nie rozwiązanym bytem statku.
- `high_speed_vessels` używa wyłącznie poprawnej prędkości i domyślnego progu 12 kn; próg jest konfigurowalny.
- Oracle korzysta z finalnej odpowiedzi i **nie jest metodą online**.
- Dokładne grupowanie całego korpusu po `MovementID` jest domyślnie wyłączone, ponieważ może wymagać bardzo dużego haszowania i przestrzeni spill; można je włączyć przez `"exact_movement_id_repetition": true` w konfiguracji.
- Automatyczny wybór okresu jest punktem startowym. Po pełnym przebiegu trzeba obejrzeć `audit/daily_profile.csv`, `audit/export_blocks.csv` i `audit/selected_period.json` przed zamrożeniem eksperymentu do artykułu.


## v0.2.2 hotfix
- bardziej przenośne agregacje audytu DuckDB (CASE zamiast rzutowania BOOL->INTEGER),
- bezpieczne aliasy SQL,
- automatyczne pokazanie końcówki logu po błędzie,
- selftest zachowuje katalog diagnostyczny przy awarii.
- Zalecane środowisko: Python 3.12.


## Poprawki v0.2.2

- Naprawiono alias `policy` w SQL etapu `evaluate` dla DuckDB 1.5.x.
- Selftest poprawnie rozpoznaje partycjonowane pliki snapshotów w `snapshots/data/`.
- Wynikowe CSV nadal zachowują kolumnę `policy`, więc etap raportowania pozostaje zgodny.

## v0.2.3 — contiguous-period guard

The experimental selector never merges disconnected calendar periods into one chronological split. It always selects the longest contiguous run of days that pass `min_daily_rows`. If that run is shorter than `min_experiment_days`, the run is allowed for diagnostics but `publication_period_ok=false` is written to `audit/selected_period.json` and the report marks the result as diagnostic only.

## v0.2.4 — odporność na strukturalnie uszkodzone linie CSV

Pełny eksport AIS zawiera co najmniej jedną linię o 20 polach przy schemacie 19-kolumnowym
(np. zgłoszona przez DuckDB linia 27,143,668). Wersja 0.2.4 nie używa
`ignore_errors=true` i nie próbuje heurystycznie przesuwać pól. Zamiast tego parser działa
z `store_rejects=true`: poprawne rekordy trafiają do Parquet, a każda strukturalnie błędna
linia jest wyłączona z analizy i zachowana do audytu w:

- `rejects/csv_reject_errors.parquet` — pełne odrzucenia z numerem linii, treścią i błędem,
- `rejects/csv_reject_scans.parquet` — parametry skanera,
- `rejects/reject_sample.csv` — pierwsze 200 wpisów do szybkiej kontroli,
- `manifests/ingest.json` — liczba odrzuconych linii, stopa odrzuceń i typy błędów.

To jest celowo konserwatywne: nie naprawiamy niejednoznacznych rekordów, aby nie wprowadzać
fałszywych wartości do timestampów, MMSI ani pozycji. W manuskrypcie należy raportować
liczbę i odsetek odrzuconych linii.


## v0.2.5 — bounded-memory snapshots

- Etap `snapshots` nie wykonuje już jednego globalnego `GROUP BY` dla całego okresu.
- Dane są przetwarzane w segmentach ograniczonych do miesiąca; jeżeli fragment nadal przekracza pamięć, jest automatycznie dzielony na mniejsze zakresy dni aż do pojedynczego dnia.
- `snapshots` używa maksymalnie 2 wątków i `preserve_insertion_order=false`, ponieważ kolejność nie ma znaczenia dla agregacji.
- Każdy ukończony fragment ma marker w `snapshots/data/_chunks/`, więc etap jest wznawialny na poziomie fragmentów.
- Definicje `message_count`, `distinct_vessels` i `high_speed_vessels`, siatka, okna i candidate delays pozostają bez zmian.
- Po aktualizacji z v0.2.4 należy użyć `--restart-stage snapshots`; ingest i audit nie są liczone ponownie.
