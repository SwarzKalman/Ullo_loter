# Üllő Lőtér – Adatkezelő (PyQt6 + Pandas)

Egyszerű, hibabiztos asztali táblázatszerkesztő Excel-alapú adatbázisokhoz. A rendszer célja, hogy ne fagyjon le, ne akadjon meg és ne veszítsen adatot még hiba esetén sem.

Főbb funkciók:
- Felhasználók, Eredmények, Versenyek szerkesztése külön füleken
- Gyors kereső a fontos mezőkben
- Gender mező validáció (M/F)
- Eredmények fülön Verseny_ID kiválasztó és automatikus kitöltés a Felhasználók alapján
- Biztonságos mentés: atomikus Excel írás, automatikus .bak mentés, hiba esetén automatikus CSV helyreállító fájl
- Háttérben történő mentés (nem blokkolja a UI-t), bezáráskor kényszerített flush

## Követelmények

- Python 3.10 vagy újabb
- Függőségek (pip):
  - pandas
  - openpyxl
  - PyQt6

Telepítéshez használja a mellékelt requirements-t:

```
pip install -r requirements.txt
```

## Indítás

A program indítása:

```
python database/table_editor.py
```

A program a következő Excel fájlokkal dolgozik a `database/` könyvtárban:
- `userDB.xlsx`
- `versenyekDB.xlsx`
- `versenyEredmenyek.xlsx`

Ha ezek nem léteznek, a program létrehozza őket az elvárt oszlopokkal.

## Hibabiztosság

A leggyakoribb adatvesztési okok ellen megerősítettük a mentési folyamatot.

1) Atomikus Excel írás
- A táblázatot először egy ideiglenes fájlba írjuk, majd `os.replace`-szel cseréljük le a végleges fájlt.
- Ez megakadályozza a részben írt, korrupt xlsx állapotokat rendszer- vagy programhiba esetén.

2) Automatikus .bak biztonsági mentés
- Minden mentés előtt készül egy `*.bak` másolat a korábbi állapotról.
- Ha a fő fájl olvashatatlan, a betöltés megpróbálja a `.bak` fájlt használni.

3) Olvasásbiztonság
- Olvasás hibája esetén (fő vagy bak fájl) üres DataFrame-et töltünk az elvárt oszlopokkal, így a program nem omlik össze.

4) Háttérben végzett mentés (UI-barát)
- A táblázat módosításai nem azonnal, hanem késleltetve és a UI szálat nem blokkolva kerülnek mentésre.
- A mentés állapota a jobb felső sarokban látható: „Mentés…”, „Mentésre vár…”, „Mentve”, illetve hiba esetén piros jelzés.

5) Hiba esetén automatikus CSV helyreállító fájl
- Ha az Excel mentés mégis hibát dob (pl. fájlzár, jogosultság), automatikusan készül egy CSV „autosave” fájl a `database/` könyvtárban.
- A fájlnév: `{eredeti_nev}.autosave-YYYYMMDD-HHMMSS.csv`. Ezt kézzel vagy programmal vissza lehet tölteni.

6) Naplózás
- Minden kivételről bejegyzés készül a `database/error.log` fájlba, a hiba részletes nyomkövetésével.

7) Kíméletes bezárás
- Ablakbezáráskor a rendszer megvárja a futó mentést, majd egy utolsó szinkron mentést végez. Hiba esetén ekkor is készít CSV helyreállítót és logot.

## Használati tippek

- Eredmények fülön új sor hozzáadásakor először a „Versenyengedelyszam” mezőt töltse ki. Ha egyezik a Felhasználók táblában lévővel, a többi rokon adat automatikusan kitöltődik és ezután szerkeszthető.
- A Gender mező csak „M” vagy „F” lehet (kisbetű megengedett, automatikusan javítjuk).

## Hibaelhárítás

- „Excel fájl foglalt” – Ha meg van nyitva Excelben, a mentés hibát dobhat. Zárja be az Excel példányt, majd a program újra menteni fog. Hiba esetén a `database/` könyvtárban keresse az automatikus CSV fájlt.
- Jogosultsági probléma – Győződjön meg róla, hogy a `database/` könyvtár írható.
- Hol találom a naplót? – `database/error.log`
- Hol találom a helyreállító fájlokat? – `database/*.autosave-*.csv`

## Fájlok és mappák

- Program fő UI:
  - `database/table_editor.py`
- Adatbázis I/O és oszlopdefiníciók:
  - `database/pandas_db.py`
- Adatfájlok:
  - `database/userDB.xlsx`
  - `database/versenyekDB.xlsx`
  - `database/versenyEredmenyek.xlsx`
  - napló: `database/error.log`
  - helyreállító CSV-k: `database/*.autosave-*.csv`

## Verzió és licenc

Belső használatra. Kódmódosításokat a fenti elvek szerint érdemes végezni (atomikus írás, backup, naplózás, UI-t nem blokkoló műveletek).