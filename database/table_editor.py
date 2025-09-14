import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QPushButton, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, QLineEdit, QLabel, QComboBox
)
from PyQt6.QtCore import Qt, QTimer, QThread, pyqtSignal, QObject
from PyQt6.QtGui import QColor
import pandas as pd
from pandas_db import (
    load_db, save_db, load_eredmeny_db, save_eredmeny_db,
    load_versenyek_db, save_versenyek_db,
    COLUMNS, EREDMENY_COLUMNS, VERSENYEK_COLUMNS,
    DB_DIR, DB_FILE, EREDMENY_DB_FILE, VERSENYEK_DB_FILE
)
from datetime import datetime
import os
import time
import traceback


def log_error(msg: str, detail: str = ""):
    """Egységes hibalog az adatbázis könyvtárba."""
    try:
        os.makedirs(DB_DIR, exist_ok=True)
        path = os.path.join(DB_DIR, "error.log")
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n{detail}\n\n")
    except Exception:
        # Logging hiba esetén sem akadjon meg a program
        pass


def _target_path_for_save_func(save_func):
    if save_func is save_db:
        return DB_FILE
    if save_func is save_eredmeny_db:
        return EREDMENY_DB_FILE
    if save_func is save_versenyek_db:
        return VERSENYEK_DB_FILE
    return os.path.join(DB_DIR, "ismeretlen.xlsx")


def _write_recovery_csv(df: pd.DataFrame, base_path: str) -> str | None:
    """Helyreállító CSV mentés, ha az Excel mentés elbukik."""
    try:
        base = os.path.basename(base_path)
        name = f"{base}.autosave-{datetime.now().strftime('%Y%m%d-%H%M%S')}.csv"
        out_path = os.path.join(DB_DIR, name)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        return out_path
    except Exception:
        log_error("Autosave CSV írási hiba", traceback.format_exc())
        return None


class SaveWorker(QObject):
    finished = pyqtSignal(bool, str)  # success, error detail (traceback)

    def __init__(self, save_func, df, parent=None):
        super().__init__(parent)
        self.save_func = save_func
        self.df = df

    def run(self):
        try:
            self.save_func(self.df)
            self.finished.emit(True, "")
        except Exception:
            self.finished.emit(False, traceback.format_exc())


class TableTab(QWidget):
    def __init__(self, load_func, save_func, columns, parent=None, update_last_changed_col=None, gender_col=None, enable_search=False, autofill_from_users=None, versenyid_selector=None):
        super().__init__(parent)
        self.load_func = load_func
        self.save_func = save_func
        self.columns = columns
        self.update_last_changed_col = update_last_changed_col
        self.gender_col = gender_col
        self.enable_search = enable_search
        self.autofill_from_users = autofill_from_users  # callable returning DataFrame, or None
        self._block_save = False
        self._editing_cell = None  # (row, col) tuple if editing
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._do_save_changes)
        self._pending_save = False
        self._last_value = None  # (row, col, value) for edit tracking
        self._pending_label = QLabel("")
        self._pending_label.setStyleSheet("color: orange; font-weight: bold;")
        self._pending_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.versenyid_selector = versenyid_selector  # QComboBox vagy None
        self.selected_versenyid = None

        # Async save state
        self._save_in_progress = False
        self._thread = None
        self._worker = None

        # Autofill state (track exactly one pending new row to autofill)
        self._autofill_target_row = None

        # Hiba-pop-up ritkítás
        self._last_error_popup_ts = 0.0

        # Látható sor -> self.df index mapping (szűréshez, szerkesztéshez)
        self._row_index_map: list[int | None] = []

        # Adatok betöltése hibavédelemmel
        try:
            self.df = self.load_func()
        except Exception:
            log_error("Adatbázis betöltési hiba", traceback.format_exc())
            self.df = pd.DataFrame(columns=self.columns)
            try:
                QMessageBox.warning(
                    self,
                    "Betöltési hiba",
                    "Nem sikerült betölteni az adatbázist. Üres táblával indul a program.\nRészletek: database/error.log"
                )
            except Exception:
                pass

        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()
        if self.enable_search:
            search_layout = QHBoxLayout()
            search_label = QLabel("Keresés:")
            self.search_edit = QLineEdit()
            self.search_edit.setPlaceholderText("Írj be keresendő szöveget...")
            self.search_edit.textChanged.connect(self.on_search)
            search_layout.addWidget(search_label)
            search_layout.addWidget(self.search_edit)
            layout.addLayout(search_layout)
        # VersenyID selector csak az eredmények tabon
        if self.versenyid_selector:
            versenyid_layout = QHBoxLayout()
            versenyid_label = QLabel("Verseny_ID:")
            versenyid_layout.addWidget(versenyid_label)
            versenyid_layout.addWidget(self.versenyid_selector)
            versenyid_layout.addStretch()
            layout.addLayout(versenyid_layout)
            self.versenyid_selector.currentIndexChanged.connect(self.on_versenyid_changed)
            self.selected_versenyid = self.versenyid_selector.currentText()
        self.table = QTableWidget()
        self.table.setColumnCount(len(self.columns))
        wrapped_headers = [self.wrap_header(col) for col in self.columns]
        self.table.setHorizontalHeaderLabels(wrapped_headers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.load_data()
        layout.addWidget(self.table)

        btn_layout = QHBoxLayout()
        add_btn = QPushButton("Új sor hozzáadása")
        add_btn.clicked.connect(self.add_row)
        btn_layout.addWidget(add_btn)
        layout.addLayout(btn_layout)
        self.setLayout(layout)

        # Pending save label a jobb felső sarokban
        pending_layout = QHBoxLayout()
        pending_layout.addStretch()
        pending_layout.addWidget(self._pending_label)
        layout.addLayout(pending_layout)

        self.table.itemChanged.connect(self.on_item_changed)
        self.table.cellActivated.connect(self.on_cell_activated)
        self.table.cellDoubleClicked.connect(self.on_cell_activated)
        self.table.itemSelectionChanged.connect(self.on_selection_changed)

    def wrap_header(self, text, max_len=10):
        # Egyszerű sortörés: max_len karakternél vág, szóköznél vagy _ után is törhet
        words = []
        current = ""
        for c in text:
            current += c
            if len(current) >= max_len or c in " _":
                words.append(current)
                current = ""
        if current:
            words.append(current)
        return "\n".join(words)

    def load_data(self, filtered_df=None):
        self._block_save = True
        if filtered_df is not None:
            # Szűrt nézet megjelenítése. Feltételezzük, hogy on_search beállította a _row_index_map-et.
            df = filtered_df
            if len(self._row_index_map) != len(df):
                # Biztonsági fallback: identitás hozzárendelés (nem ideális szűrt nézethez, de elkerüli a hibát)
                self._row_index_map = list(range(len(df)))
        else:
            try:
                self.df = self.load_func()
            except Exception:
                log_error("Adatok újratöltési hiba", traceback.format_exc())
                # maradjon a régi self.df
            df = self.df
            # Nem szűrt: identitás hozzárendelés
            self._row_index_map = list(range(len(df)))

        self.table.setRowCount(len(df))
        alt_color = QColor(33, 33, 33)  # sötétebb szürke
        for row in range(len(df)):
            for col, col_name in enumerate(self.columns):
                value = df.iloc[row][col_name] if col_name in df.columns else ""
                if pd.isna(value):
                    value = ""
                else:
                    value = str(value)
                item = QTableWidgetItem(value)
                # Minden második sor háttérszínét állítjuk
                if row % 2 == 1:
                    item.setBackground(alt_color)
                self.table.setItem(row, col, item)
        self._block_save = False

    def _df_index_for_row(self, row: int) -> int:
        """
        A látható 'row' táblázatsorhoz visszaadja a self.df indexét.
        Ha a sor új (nincs hozzárendelve), akkor self.df-hez hozzáad egy üres sort és létrehozza a hozzárendelést.
        """
        if 0 <= row < len(self._row_index_map) and self._row_index_map[row] is not None:
            return int(self._row_index_map[row])  # type: ignore
        # Új sor: bővítsük a df-et és a map-et
        new_idx = len(self.df)
        new_row = {col: "" for col in self.columns}
        self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
        while len(self._row_index_map) <= row:
            self._row_index_map.append(None)
        self._row_index_map[row] = new_idx
        return new_idx

    def on_versenyid_changed(self, idx):
        if self.versenyid_selector:
            self.selected_versenyid = self.versenyid_selector.currentText()

    def add_row(self):
        row_pos = self.table.rowCount()
        self.table.insertRow(row_pos)
        for col in range(self.table.columnCount()):
            self.table.setItem(row_pos, col, QTableWidgetItem(""))
        # Ha van versenyid_selector és "Verseny_ID" oszlop, akkor automatikusan kitöltjük
        if self.versenyid_selector and "Verseny_ID" in self.columns:
            idx = self.columns.index("Verseny_ID")
            versenyid = self.selected_versenyid or ""
            self.table.setItem(row_pos, idx, QTableWidgetItem(versenyid))
        # Új táblázatsorhoz még nincs df index, jelöljük None-nal
        if len(self._row_index_map) < self.table.rowCount():
            self._row_index_map.append(None)
        # Autofill: lock all cells except first until azonosító beírása megtörténik
        if self.autofill_from_users:
            self._autofill_target_row = row_pos
            for col in range(1, self.table.columnCount()):
                item = self.table.item(row_pos, col)
                if item:
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)

    def on_cell_activated(self, row, col):
        # Ha szerkesztés indul, állítsuk a szöveg színét a kijelölt cella háttérszínére
        item = self.table.item(row, col)
        if item:
            # A kiválasztott cella háttérszíne
            sel_bg_color = self.table.palette().color(self.table.palette().ColorRole.Highlight)
            item.setForeground(sel_bg_color)
            self._editing_cell = (row, col)
            # Elmentjük az aktuális értéket, hogy össze tudjuk hasonlítani mentéskor
            self._last_value = (row, col, item.text())

    def on_selection_changed(self):
        # Ha elhagyjuk a szerkesztett cellát, visszaállítjuk a szöveg színét az alapértelmezettre
        if self._editing_cell:
            row, col = self._editing_cell
            item = self.table.item(row, col)
            if item:
                default_color = self.table.palette().color(self.table.foregroundRole())
                item.setForeground(default_color)
            self._editing_cell = None

    def on_item_changed(self, item):
        if self._block_save:
            return
        row = item.row()
        col = item.column()
        col_name = self.columns[col]

        # A módosított látható sorhoz tartozó df index
        df_idx = self._df_index_for_row(row)

        # Eredeti érték a df-ből még azelőtt, hogy self.df-t frissítenénk
        prev_val_df = ""
        if df_idx < len(self.df):
            prev_raw = self.df.iloc[df_idx][col_name] if col_name in self.df.columns else ""
            prev_val_df = "" if pd.isna(prev_raw) else str(prev_raw)
        # Autofill kezelés: csak ha az új sor első celláját töltötték ki
        if self.autofill_from_users and self._autofill_target_row is not None and row == self._autofill_target_row and col == 0:
            versenyengedelyszam = item.text().strip()
            if versenyengedelyszam:
                try:
                    users_df = self.autofill_from_users()
                except Exception:
                    users_df = pd.DataFrame(columns=["Versenyengedelyszam"])
                    log_error("Autofill felhasználó betöltési hiba", traceback.format_exc())
                match = users_df[users_df["Versenyengedelyszam"].astype(str) == versenyengedelyszam] if "Versenyengedelyszam" in users_df.columns else pd.DataFrame()
                if not match.empty:
                    user_row = match.iloc[0]
                    # Töltsük ki az ismert adatokat (GUI + self.df)
                    for col_idx, col_name2 in enumerate(self.columns):
                        if col_idx == 0:
                            continue  # Az első cella már ki van töltve
                        if col_name2 in user_row:
                            value = "" if pd.isna(user_row[col_name2]) else str(user_row[col_name2])
                            self._block_save = True
                            self.table.setItem(row, col_idx, QTableWidgetItem(value))
                            self._block_save = False
                            # frissítsük az alap df-et is
                            self.df.at[df_idx, col_name2] = value
                    # Most már szerkeszthetővé tesszük a többi cellát is
                    for col_idx in range(1, self.table.columnCount()):
                        item2 = self.table.item(row, col_idx)
                        if item2:
                            item2.setFlags(item2.flags() | Qt.ItemFlag.ItemIsEditable)
                # Akár volt találat, akár nem, az autofill célsor lezárul
                self._autofill_target_row = None

        # Gender validáció csak ha van gender_col beállítva
        if self.gender_col and col_name == self.gender_col:
            val = item.text().strip()
            if val.lower() == "m":
                val = "M"
            elif val.lower() == "f":
                val = "F"
            elif val == "":
                val = ""
            else:
                QMessageBox.warning(
                    self,
                    "Hibás nem",
                    "A Gender mező csak M=Male vagy F=Female lehet!\nEgyéb gendert a rendszer nem kezel."
                )
                self._block_save = True
                prev_val = self.df.iloc[df_idx][col_name] if df_idx < len(self.df) else ""
                if pd.isna(prev_val):
                    prev_val = ""
                # IMPORTANT: ne cseréljünk item objektumot, csak a szövegét állítsuk
                self.table.blockSignals(True)
                item.setText(str(prev_val))
                self.table.blockSignals(False)
                self._block_save = False
                # self.df már az előző értéket tartalmazza, nincs további teendő
                return
            # Normalizált érték beállítása (GUI + self.df)
            self._block_save = True
            self.table.blockSignals(True)
            item.setText(val)
            self.table.blockSignals(False)
            self._block_save = False
            self.df.at[df_idx, col_name] = val
        else:
            # Nem gender oszlop: az aktuális item értéke menjen az alap df-be
            self.df.at[df_idx, col_name] = item.text()

        # Csak akkor frissítsük Last_changed-et, ha ténylegesen változott az érték
        # Last_changed frissítés csak ha ténylegesen változott az érték
        if self.update_last_changed_col and col_name != self.update_last_changed_col and item.text() != prev_val_df:
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            last_changed_idx = self.columns.index(self.update_last_changed_col)
            # GUI frissítés jelblokkolással
            self.table.blockSignals(True)
            self.table.setItem(row, last_changed_idx, QTableWidgetItem(now))
            self.table.blockSignals(False)
            # self.df frissítése
            self.df.at[df_idx, self.update_last_changed_col] = now

        self.schedule_save_changes()
        # Mentés után a szerkesztett cella színét is visszaállítjuk alapértelmezettre
        if self._editing_cell:
            row2, col2 = self._editing_cell
            item2 = self.table.item(row2, col2)
            if item2:
                default_color = self.table.palette().color(self.table.foregroundRole())
                item2.setForeground(default_color)
            self._editing_cell = None
        self._last_value = None

    def schedule_save_changes(self):
        # Ha már fut a késleltetett mentő timer vagy éppen mentünk, csak jelezzük a pending állapotot
        if self._save_timer.isActive() or self._save_in_progress:
            self._pending_save = True
            self._pending_label.setText("Mentésre vár…")
        else:
            self._pending_save = False
            self._pending_label.setText("")
            self._save_timer.start(10000)  # 10 másodperc

    def _do_save_changes(self):
        self.save_changes()

    def _snapshot_df(self):
        """
        A teljes adatállapot visszaadása (nem a látható táblanézetből építve),
        hogy a szűrt nézet ne vágja le a nem látható sorokat mentéskor.
        """
        return self.df.copy()

    def _start_async_save(self, df):
        if self._save_in_progress:
            # már folyamatban van mentés; jelezzük, hogy utána ismét mentsen
            self._pending_save = True
            self._pending_label.setText("Mentésre vár…")
            return
        self._save_in_progress = True
        self._pending_label.setStyleSheet("color: orange; font-weight: bold;")
        self._pending_label.setText("Mentés…")

        self._thread = QThread(self)
        self._worker = SaveWorker(self.save_func, df)
        self._worker.moveToThread(self._thread)
        self._thread.started.connect(self._worker.run)
        self._worker.finished.connect(self._on_save_finished)
        self._worker.finished.connect(self._thread.quit)
        self._worker.finished.connect(self._worker.deleteLater)
        self._thread.finished.connect(self._thread.deleteLater)
        self._thread.start()

    def _on_save_finished(self, ok, err_detail):
        self._save_in_progress = False
        if ok:
            self._pending_label.setStyleSheet("color: #4CAF50; font-weight: bold;")  # green
            self._pending_label.setText("Mentve")
            # Rövid idő után vissza narancs és ürítés
            QTimer.singleShot(2000, lambda: self._pending_label.setText(""))
            QTimer.singleShot(1, lambda: self._pending_label.setStyleSheet("color: orange; font-weight: bold;"))
            # Ha közben érkezett újabb változtatás, indítsuk újra a késleltetett mentést
            if self._pending_save:
                self._pending_save = False
                # rövid várakozással gyűjtsük össze a friss módosításokat
                self._save_timer.start(2000)
        else:
            # Hiba kezelése: log + helyreállító CSV + felhasználói tájékoztatás (ritkított pop-up)
            log_error("Mentési hiba", err_detail)
            df_snapshot = self._snapshot_df()
            target_path = _target_path_for_save_func(self.save_func)
            recovery_path = _write_recovery_csv(df_snapshot, target_path)
            self._pending_label.setStyleSheet("color: red; font-weight: bold;")
            if recovery_path:
                self._pending_label.setText(f"Mentési hiba. Helyreállító fájl: {os.path.basename(recovery_path)}")
            else:
                self._pending_label.setText("Mentési hiba. Részletek: database/error.log")
            now_ts = time.time()
            if now_ts - self._last_error_popup_ts > 60.0:
                self._last_error_popup_ts = now_ts
                try:
                    msg = "Nem sikerült menteni a(z) Excel fájlba.\n"
                    if recovery_path:
                        msg += f"Helyreállító CSV készült: {recovery_path}\n"
                    msg += "Részletek: database/error.log"
                    QMessageBox.critical(self, "Mentési hiba", msg)
                except Exception:
                    pass

    def save_changes(self):
        # Mindig a belső self.df állapotot mentsük, így a szűrt nézet nem vágja le a többi sort
        df_new = self._snapshot_df()
        self._start_async_save(df_new)

    def on_search(self, text):
        # Gyors keresés: csak a megadott oszlopokban keres
        try:
            search = text.strip().lower()
            if not search:
                # törölt szűrés: töltsük vissza a teljes táblát és állítsuk identitás hozzárendelést
                self._row_index_map = list(range(len(self.df)))
                self.load_data()
                return
            # Csak ezekben az oszlopokban keresünk
            search_cols = ["Versenyengedelyszam", "Name", "Phone number", "Email"]
            # Ellenőrizzük, hogy ezek az oszlopok léteznek-e
            valid_cols = [col for col in search_cols if col in self.df.columns]
            if not valid_cols:
                # Ha nincs érvényes oszlop, üres találati lista
                self._row_index_map = []
                self.load_data(filtered_df=self.df.iloc[[]])
                return
            arr = self.df[valid_cols].fillna("").astype(str).values
            mask = [any(search in cell.lower() for cell in row) for row in arr]
            # Látható sorok -> eredeti df indexek
            mapping = [i for i, m in enumerate(mask) if m]
            filtered_df = self.df.iloc[mapping].reset_index(drop=True)
            # Állítsuk be a mappinget a szűrt nézethez
            self._row_index_map = mapping
            self.load_data(filtered_df=filtered_df)
        except Exception:
            log_error("Keresési hiba", traceback.format_exc())

    def flush_and_wait(self, timeout_ms=5000):
        """
        Bezáráskor szinkron mentés, hogy adatvesztést elkerüljük.
        Rövid ideig várunk a háttérmentésre, majd szükség esetén szinkronban mentünk.
        """
        try:
            if self._save_timer.isActive():
                self._save_timer.stop()
            # Adjunk esélyt a futó mentésnek, hogy befejeződjön
            t0 = time.time()
            while self._save_in_progress and (time.time() - t0) < (timeout_ms / 1000.0):
                QApplication.processEvents()
                time.sleep(0.05)
            # Mindig a teljes df-et mentsük, ne a látható táblát
            df_new = self.df.copy()
            try:
                self.save_func(df_new)
            except Exception:
                # Ha itt is elbukik, legalább legyen helyreállító CSV és log
                log_error("Záráskor szinkron mentési hiba", traceback.format_exc())
                _write_recovery_csv(df_new, _target_path_for_save_func(self.save_func))
        except Exception:
            # Utolsó esély jelzés; itt már zárunk, de legalább látjuk a hibát
            log_error("flush_and_wait hiba", traceback.format_exc())
            self._pending_label.setStyleSheet("color: red; font-weight: bold;")
            self._pending_label.setText("Mentési hiba záráskor. Részletek: database/error.log")


class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Adatbázis szerkesztő")
        self.resize(1200, 600)
        layout = QVBoxLayout()
        tabs = QTabWidget()
        # Felhasználók tab: Last_changed automatikus frissítés, Gender validáció, kereső
        self.users_tab = TableTab(
            load_db, save_db, COLUMNS,
            update_last_changed_col="Last_changed",
            gender_col="Gender",
            enable_search=True
        )
        tabs.addTab(self.users_tab, "Felhasználók")
        # Eredmények tab: kereső, autofill, versenyID selector
        if "Kategoria" not in EREDMENY_COLUMNS:
            EREDMENY_COLUMNS.append("Kategoria")
        # Verseny_ID selector előkészítése
        try:
            versenyek_df = load_versenyek_db()
        except Exception:
            log_error("Versenyek DB betöltési hiba", traceback.format_exc())
            versenyek_df = pd.DataFrame(columns=["Verseny_ID"])
        versenyid_list = [str(v) for v in versenyek_df["Verseny_ID"].dropna().unique() if str(v).strip()] if "Verseny_ID" in versenyek_df.columns else []
        versenyid_selector = QComboBox()
        versenyid_selector.addItems(versenyid_list)
        self.eredmeny_tab = TableTab(
            load_eredmeny_db, save_eredmeny_db, EREDMENY_COLUMNS,
            enable_search=True,
            autofill_from_users=load_db,
            versenyid_selector=versenyid_selector
        )
        tabs.addTab(self.eredmeny_tab, "Eredmények")
        self.versenyek_tab = TableTab(load_versenyek_db, save_versenyek_db, VERSENYEK_COLUMNS)
        tabs.addTab(self.versenyek_tab, "Versenyek")
        layout.addWidget(tabs)
        self.setLayout(layout)

    def closeEvent(self, event):
        # Zárás előtt próbáljuk a változásokat diszken biztosan rögzíteni
        try:
            self.users_tab.flush_and_wait(5000)
        except Exception:
            pass
        try:
            self.eredmeny_tab.flush_and_wait(5000)
        except Exception:
            pass
        try:
            self.versenyek_tab.flush_and_wait(5000)
        except Exception:
            pass
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    mw = MainWindow()
    mw.show()
    sys.exit(app.exec())
