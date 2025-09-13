import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QPushButton, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QHeaderView, QMessageBox, QLineEdit, QLabel
)
from PyQt6.QtCore import Qt, QTimer
from PyQt6.QtGui import QColor
import pandas as pd
from pandas_db import (
    load_db, save_db, load_eredmeny_db, save_eredmeny_db,
    load_versenyek_db, save_versenyek_db,
    COLUMNS, EREDMENY_COLUMNS, VERSENYEK_COLUMNS
)
from datetime import datetime

class TableTab(QWidget):
    def __init__(self, load_func, save_func, columns, parent=None, update_last_changed_col=None, gender_col=None, enable_search=False, autofill_from_users=None):
        super().__init__(parent)
        self.load_func = load_func
        self.save_func = save_func
        self.columns = columns
        self.update_last_changed_col = update_last_changed_col
        self.gender_col = gender_col
        self.enable_search = enable_search
        self.autofill_from_users = autofill_from_users  # DataFrame of users, or None
        self.df = self.load_func()
        self._block_save = False
        self._editing_cell = None  # (row, col) tuple if editing
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._do_save_changes)
        self._pending_save = False
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
            df = filtered_df
        else:
            self.df = self.load_func()
            df = self.df
        self.table.setRowCount(len(df))
        for row in range(len(df)):
            for col, col_name in enumerate(self.columns):
                value = df.iloc[row][col_name] if col_name in df.columns else ""
                if pd.isna(value):
                    value = ""
                else:
                    value = str(value)
                self.table.setItem(row, col, QTableWidgetItem(value))
        self._block_save = False

    def add_row(self):
        row_pos = self.table.rowCount()
        self.table.insertRow(row_pos)
        for col in range(self.table.columnCount()):
            self.table.setItem(row_pos, col, QTableWidgetItem(""))
        # Ha autofill_from_users aktív, csak az első cella (Versenyengedelyszam) legyen szerkeszthető, a többi zárolt
        if self.autofill_from_users:
            for col in range(1, self.table.columnCount()):
                item = self.table.item(row_pos, col)
                if item:
                    item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            # Figyeljük az első cella szerkesztését
            self.table.itemChanged.connect(self.on_autofill_item_changed)

    def on_autofill_item_changed(self, item):
        # Csak akkor fut, ha autofill_from_users aktív
        if self._block_save:
            return
        row = item.row()
        col = item.column()
        if row != self.table.rowCount() - 1 or col != 0:
            return  # Csak az új sor első cellájára reagálunk
        versenyengedelyszam = item.text().strip()
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
                    self.table.setItem(row, col_idx, QTableWidgetItem(value))
                    self._block_save = False
            # Most már szerkeszthetővé tesszük a többi cellát is
            for col_idx in range(1, self.table.columnCount()):
                item2 = self.table.item(row, col_idx)
                if item2:
                    item2.setFlags(item2.flags() | Qt.ItemFlag.ItemIsEditable)
        # Leválasztjuk ezt a handler-t, hogy csak az új sor első cellájára hasson
        self.table.itemChanged.disconnect(self.on_autofill_item_changed)

    def on_cell_activated(self, row, col):
        # Ha szerkesztés indul, állítsuk a szöveg színét a kijelölt cella háttérszínére
        item = self.table.item(row, col)
        if item:
            # A kiválasztott cella háttérszíne
            sel_bg_color = self.table.palette().color(self.table.palette().ColorRole.Highlight)
            item.setForeground(sel_bg_color)
            self._editing_cell = (row, col)

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
                prev_val = self.df.iloc[row][col_name] if row < len(self.df) else ""
                if pd.isna(prev_val):
                    prev_val = ""
                self.table.setItem(row, col, QTableWidgetItem(str(prev_val)))
                self._block_save = False
                return
            self._block_save = True
            self.table.setItem(row, col, QTableWidgetItem(val))
            self._block_save = False

        # Ha Felhasználók tab és nem a Last_changed oszlop, akkor frissítsük Last_changed-et
        if self.update_last_changed_col and col_name != self.update_last_changed_col:
            last_changed_idx = self.columns.index(self.update_last_changed_col)
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            self.table.blockSignals(True)
            self.table.setItem(row, last_changed_idx, QTableWidgetItem(now))
            self.table.blockSignals(False)
        self.schedule_save_changes()
        # Mentés után a szerkesztett cella színét is visszaállítjuk alapértelmezettre
        if self._editing_cell:
            row, col = self._editing_cell
            item = self.table.item(row, col)
            if item:
                default_color = self.table.palette().color(self.table.foregroundRole())
                item.setForeground(default_color)
            self._editing_cell = None

    def schedule_save_changes(self):
        if self._save_timer.isActive():
            self._pending_save = True
            # Timer már fut, csak jelezzük, hogy újabb mentés szükséges
        else:
            self._pending_save = False
            self._save_timer.start(10000)  # 10 másodperc

    def _do_save_changes(self):
        self.save_changes()
        if self._pending_save:
            self._pending_save = False
            self._save_timer.start(10000)  # újabb 10 másodperc, ha közben volt változás

    def save_changes(self):
        rows = self.table.rowCount()
        cols = self.table.columnCount()
        data = []
        for row in range(rows):
            row_data = {}
            for col in range(cols):
                item = self.table.item(row, col)
                val = item.text() if item else ""
                row_data[self.columns[col]] = val
            data.append(row_data)
        df_new = pd.DataFrame(data, columns=self.columns)
        self.save_func(df_new)

    def on_search(self, text):
        # Gyors keresés: csak a megadott oszlopokban keres
        search = text.strip().lower()
        if not search:
            self.load_data()
            return
        # Csak ezekben az oszlopokban keresünk
        search_cols = ["Versenyengedelyszam", "Name", "Phone number", "Email"]
        # Ellenőrizzük, hogy ezek az oszlopok léteznek-e
        valid_cols = [col for col in search_cols if col in self.df.columns]
        arr = self.df[valid_cols].fillna("").astype(str).values
        mask = [any(search in cell.lower() for cell in row) for row in arr]
        filtered_df = self.df[mask].reset_index(drop=True)
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
        # Eredmények tab: kereső és autofill a felhasználókból
        tabs.addTab(TableTab(
            load_eredmeny_db, save_eredmeny_db, EREDMENY_COLUMNS,
            enable_search=True,
            autofill_from_users=load_db
        ), "Eredmények")
        tabs.addTab(TableTab(load_versenyek_db, save_versenyek_db, VERSENYEK_COLUMNS), "Versenyek")
        layout.addWidget(tabs)
        self.setLayout(layout)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    mw = MainWindow()
    mw.show()
    sys.exit(app.exec())
    sys.exit(app.exec())
