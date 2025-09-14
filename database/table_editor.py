import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QPushButton, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, QLineEdit, QLabel, QComboBox
)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QColor
import pandas as pd
import numpy as np
from pandas_db import (
    load_db, save_db, load_eredmeny_db, save_eredmeny_db,
    load_versenyek_db, save_versenyek_db,
    COLUMNS, EREDMENY_COLUMNS, VERSENYEK_COLUMNS
)
from datetime import datetime

class TableTab(QWidget):
    def __init__(self, load_func, save_func, columns, parent=None, update_last_changed_col=None, gender_col=None, enable_search=False, autofill_from_users=None, versenyid_selector=None):
        super().__init__(parent)
        self.load_func = load_func
        self.save_func = save_func
        self.columns = columns
        self.update_last_changed_col = update_last_changed_col
        self.gender_col = gender_col
        self.enable_search = enable_search
        self.autofill_from_users = autofill_from_users  # DataFrame of users, or None
        self.df = self.load_func()
        self._displayed_df = None
        self._is_filtered = False
        self._block_save = False
        self._editing_cell = None  # (row, col) tuple if editing
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._do_save_changes)
        self._pending_save = False
        self._autofill_connected = False
        self._last_value = None  # (row, col, value) for edit tracking
        self._pending_label = QLabel("")
        self._pending_label.setStyleSheet("color: orange; font-weight: bold;")
        self._pending_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self.versenyid_selector = versenyid_selector  # QComboBox vagy None
        self.selected_versenyid = None
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
            # Keep the DataFrame exactly as provided so we retain original indices
            df = filtered_df
            self._is_filtered = True
        else:
            self.df = self.load_func()
            df = self.df
            self._is_filtered = False
        # Keep displayed_df as a copy for safe read operations (preserve index if filtered)
        self._displayed_df = df.copy()
        self.table.setRowCount(len(df))
        alt_color = QColor(33, 33, 33)  # sötétebb szürke
        for row in range(len(df)):
            for col, col_name in enumerate(self.columns):
                # Use iloc on displayed df to avoid index mismatches
                try:
                    value = self._displayed_df.iloc[row][col_name] if col_name in self._displayed_df.columns else ""
                except Exception:
                    value = ""
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
        # Ha autofill_from_users aktív, csak az első cella (Versenyengedelyszam) legyen szerkeszthető, a többi zárolt
        if self.autofill_from_users:
            for col in range(1, self.table.columnCount()):
                item = self.table.item(row_pos, col)
                if item:
                    # make cell non-editable for newly added row until autofill completes
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.table.itemChanged.connect(self.on_autofill_item_changed)

    def on_autofill_item_changed(self, item):
        # Csak akkor fut, ha autofill_from_users aktív
        if self._block_save:
            return
        row = item.row()
        col = item.column()
        if row != self.table.rowCount() - 1 or col != 0:
            return  # Csak az új sor első cellájára reagálunk
        # Capture text safely (item might be deleted/replaced)
        try:
            versenyengedelyszam = item.text().strip()
        except RuntimeError:
            return
        if not versenyengedelyszam:
            return
        # Keresés a felhasználók között
        users_df = self.autofill_from_users()
        match = users_df[users_df["Versenyengedelyszam"].astype(str) == versenyengedelyszam]
        if not match.empty:
            user_row = match.iloc[0]
            # Töltsük ki az ismert adatokat
            for col_idx, col_name in enumerate(self.columns):
                if col_idx == 0:
                    continue  # Az első cella már ki van töltve
                if col_name in user_row:
                    value = "" if pd.isna(user_row[col_name]) else str(user_row[col_name])
                    self._block_save = True
                    # Block signals to avoid re-entrancy
                    self.table.blockSignals(True)
                    self.table.setItem(row, col_idx, QTableWidgetItem(value))
                    self.table.blockSignals(False)
                    self._block_save = False
            # Most már szerkeszthetővé tesszük a többi cellát is
            for col_idx in range(1, self.table.columnCount()):
                item2 = self.table.item(row, col_idx)
                if item2:
                    item2.setFlags(item2.flags() | Qt.ItemFlag.ItemIsEditable)
        # Leválasztjuk ezt a handler-t, hogy csak az új sor első cellájára hasson
        try:
            self.table.itemChanged.disconnect(self.on_autofill_item_changed)
        except Exception:
            pass

    def on_cell_activated(self, row, col):
        # Ha szerkesztés indul, állítsuk a szöveg színét a kijelölt cella háttérszínére
        item = self.table.item(row, col)
        if item:
            # A kiválasztott cella háttérszíne
            sel_bg_color = self.table.palette().color(self.table.palette().ColorRole.Highlight)
            item.setForeground(sel_bg_color)
            self._editing_cell = (row, col)
            # Elmentjük az aktuális értéket, hogy össze tudjuk hasonlítani mentéskor
            # Capture item.text() early to avoid future deletion issues
            try:
                txt = item.text()
            except RuntimeError:
                txt = ""
            self._last_value = (row, col, txt)

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
        # Capture text early to avoid using an object that may be replaced/deleted later
        try:
            item_text = item.text()
        except RuntimeError:
            # Item was deleted/replaced; nothing to do
            return
        row = item.row()
        col = item.column()
        col_name = self.columns[col]

        # Gender validáció csak ha van gender_col beállítva
        if self.gender_col and col_name == self.gender_col:
            val = item_text.strip()
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
                prev_val = ""
                # Use displayed_df when filtered to get the correct row mapping
                try:
                    if self._is_filtered and self._displayed_df is not None and row < len(self._displayed_df):
                        prev_val = self._displayed_df.iloc[row][col_name] if col_name in self._displayed_df.columns else ""
                    elif row < len(self.df):
                        prev_val = self.df.iloc[row][col_name] if col_name in self.df.columns else ""
                except Exception:
                    prev_val = ""
                if pd.isna(prev_val):
                    prev_val = ""
                self.table.blockSignals(True)
                self.table.setItem(row, col, QTableWidgetItem(str(prev_val)))
                self.table.blockSignals(False)
                self._block_save = False
                return
            self._block_save = True
            # Update item safely without triggering signals
            self.table.blockSignals(True)
            self.table.setItem(row, col, QTableWidgetItem(val))
            self.table.blockSignals(False)
            self._block_save = False

        # Csak akkor frissítsük Last_changed-et, ha ténylegesen változott az érték
        update_last_changed = True
        # Determine previous value correctly depending on filtered state
        prev_val = ""
        try:
            if self._is_filtered and self._displayed_df is not None and row < len(self._displayed_df):
                if col_name in self._displayed_df.columns:
                    prev_val = self._displayed_df.iloc[row][col_name]
            elif row < len(self.df):
                if col_name in self.df.columns:
                    prev_val = self.df.iloc[row][col_name]
            if pd.isna(prev_val):
                prev_val = ""
            else:
                prev_val = str(prev_val)
        except Exception:
            prev_val = ""
        if self.update_last_changed_col and col_name != self.update_last_changed_col:
            if prev_val == item_text:
                update_last_changed = False
            if update_last_changed:
                last_changed_idx = self.columns.index(self.update_last_changed_col)
                now = datetime.now().strftime("%Y-%m-%d %H:%M")
                self.table.blockSignals(True)
                self.table.setItem(row, last_changed_idx, QTableWidgetItem(now))
                self.table.blockSignals(False)
        self.schedule_save_changes()
        # Mentés után a szerkesztett cella színét is visszaállítjuk alapértelmezettre
        if self._editing_cell:
            rowe, cole = self._editing_cell
            item2 = self.table.item(rowe, cole)
            if item2:
                default_color = self.table.palette().color(self.table.foregroundRole())
                item2.setForeground(default_color)
            self._editing_cell = None
        self._last_value = None

    def schedule_save_changes(self):
        if self._save_timer.isActive():
            self._pending_save = True
            self._pending_label.setText("Mentésre vár…")
        else:
            self._pending_save = False
            self._pending_label.setText("")
            self._save_timer.start(10000)  # 10 másodperc

    def _do_save_changes(self):
        self.save_changes()
        if self._pending_save:
            self._pending_save = False
            self._pending_label.setText("")
            self._save_timer.start(10000)  # újabb 10 másodperc, ha közben volt változás
        else:
            self._pending_label.setText("")

    def save_changes(self):
        # Készítsünk DataFrame-et a jelenlegi táblából
        rows = self.table.rowCount()
        cols = self.table.columnCount()
        data = []
        for row in range(rows):
            row_data = {}
            for col in range(cols):
                item = self.table.item(row, col)
                # Capture item text safely; item may have been deleted/replaced
                val = ""
                if item:
                    try:
                        val = item.text()
                    except RuntimeError:
                        val = ""
                row_data[self.columns[col]] = val
            data.append(row_data)
        df_table = pd.DataFrame(data, columns=self.columns)

        # Ha a teljes df látható, egyszerűen mentsük
        if not self._is_filtered or self._displayed_df is None or len(self._displayed_df) == len(self.df):
            # Update internal df to df_table to keep in memory consistent
            self.df = df_table.copy()
            self.save_func(self.df)
            return

        # Szűrt nézetből mentés: próbáljuk meg azonosító alapján (Versenyengedelyszam) merge-elni
        key = "Versenyengedelyszam"
        if key in self.df.columns and key in df_table.columns:
            # Normalize types to string for comparison to avoid dtype issues
            left = self.df.copy()
            right = df_table.copy()
            left[key] = left[key].astype(str).fillna("")
            right[key] = right[key].astype(str).fillna("")
            # For rows in right, update corresponding rows in left where key matches
            for _, r in right.iterrows():
                k = r[key]
                # Build normalized row dict with proper types to avoid dtype conflicts
                row_dict = {}
                for col in self.columns:
                    v = r[col]
                    # If left doesn't have the column, just keep v as-is
                    if col not in left.columns:
                        row_dict[col] = v
                        continue
                    # Empty strings should be NaN for numeric columns
                    if pd.isna(v) or (isinstance(v, str) and v == ""):
                        if pd.api.types.is_numeric_dtype(left[col]):
                            row_dict[col] = np.nan
                        else:
                            row_dict[col] = "" if not pd.api.types.is_float_dtype(left[col]) else np.nan
                    else:
                        # Try to coerce numeric columns
                        if pd.api.types.is_numeric_dtype(left[col]):
                            row_dict[col] = pd.to_numeric(v, errors='coerce')
                        elif pd.api.types.is_datetime64_any_dtype(left[col]):
                            try:
                                row_dict[col] = pd.to_datetime(v, errors='coerce')
                            except Exception:
                                row_dict[col] = v
                        else:
                            row_dict[col] = v
                if k == "" or k not in left[key].values:
                    # New row or unknown key -> append as new
                    left = pd.concat([left, pd.DataFrame([row_dict])], ignore_index=True)
                else:
                    idxs = left.index[left[key] == k].tolist()
                    # Update first matching index
                    if idxs:
                        idx = idxs[0]
                        for col in self.columns:
                            val = row_dict.get(col, "")
                            left.at[idx, col] = val
            # Save merged dataframe
            self.df = left
            self.save_func(self.df)
            return

        # Ha nincs egyedi kulcs, ne írjuk felül a teljes adatbázist véletlenül
        QMessageBox.warning(
            self,
            "Mentés megtagadva",
            "Nem lehet biztonságosan menteni szűrt nézetből, mert nincs egyedi azonosító (Versenyengedelyszam). "
            "Kérlek töröld a szűrőt, majd próbáld újra a mentést."
        )

    def on_search(self, text):
        # Gyors keresés: csak a megadott oszlopokban keres
        search = text.strip()
        if not search:
            self.load_data()
            return
        search_lower = search.lower()
        # Csak ezek az oszlopokban keresünk
        search_cols = ["Versenyengedelyszam", "Name", "Phone number", "Email"]
        # Ellenőrizzük, hogy ezek az oszlopok léteznek-e
        valid_cols = [col for col in search_cols if col in self.df.columns]
        if not valid_cols:
            # Nincs érvényes oszlop, ne próbáljuk meg a cellánkénti iterálást
            self.load_data()
            return
        # Vektorizált, biztonságos keresés minden érvényes oszlopon
        df_text = self.df[valid_cols].fillna("").astype(str)
        # contains_df: boolean DataFrame where True if cell contains search (case-insensitive)
        contains_df = df_text.apply(lambda col: col.str.lower().str.contains(search_lower, na=False))
        mask = contains_df.any(axis=1)
        # Keep original indices so we can map back edits correctly
        filtered_df = self.df.loc[mask]
        self.load_data(filtered_df=filtered_df)

class MainWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Adatbázis szerkesztő")
        self.resize(1200, 600)
        layout = QVBoxLayout()
        tabs = QTabWidget()
        # Felhasználók tab: Last_changed automatikus frissítés, Gender validáció, kereső
        tabs.addTab(TableTab(
            load_db, save_db, COLUMNS,
            update_last_changed_col="Last_changed",
            gender_col="Gender",
            enable_search=True
        ), "Felhasználók")
        # Eredmények tab: kereső, autofill, versenyID selector
        if "Kategoria" not in EREDMENY_COLUMNS:
            EREDMENY_COLUMNS.append("Kategoria")
        # Verseny_ID selector előkészítése
        versenyek_df = load_versenyek_db()
        versenyid_list = [str(v) for v in versenyek_df["Verseny_ID"].dropna().unique() if str(v).strip()]
        versenyid_selector = QComboBox()
        versenyid_selector.addItems(versenyid_list)
        tabs.addTab(TableTab(
            load_eredmeny_db, save_eredmeny_db, EREDMENY_COLUMNS,
            enable_search=True,
            autofill_from_users=load_db,
            versenyid_selector=versenyid_selector
        ), "Eredmények")
        tabs.addTab(TableTab(load_versenyek_db, save_versenyek_db, VERSENYEK_COLUMNS), "Versenyek")
        layout.addWidget(tabs)
        self.setLayout(layout)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    mw = MainWindow()
    mw.show()
    sys.exit(app.exec())
