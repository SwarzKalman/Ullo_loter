import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QTabWidget, QVBoxLayout, QPushButton, QHBoxLayout,
    QTableView, QMessageBox, QLineEdit, QLabel, QComboBox
)
from PyQt6.QtCore import (
    Qt, QTimer, QThread, pyqtSignal, QObject, QAbstractTableModel,
    QModelIndex, QSortFilterProxyModel
)
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


# --------------------------
#   Model / Proxy réteg
# --------------------------

class PandasTableModel(QAbstractTableModel):
    """
    QAbstractTableModel pandas DataFrame-hez.
    - Szerkeszthető
    - Gender validáció (M/F/üres)
    - Last_changed automatikus frissítés
    - Keresési blob karbantartás (gyors szűréshez)
    - Autofill támogatás (első oszlop beírásakor, ha be van állítva egy users_df loader)
    - Opcionális: egyedi (A,B) kulcspár tiltja a duplikációt
    - Opcionális: 'id-like' oszlopok normalizálása (pl. '123.0' -> '123')
    - ÚJ: opcionális verseny-id getter: ha beállítva és a sor első cellájába beírtak,
           automatikusan kitölti a "Verseny_ID" oszlopot az aktuális verseny azonosítójával.
    """
    def __init__(self, df: pd.DataFrame, columns: list[str],
                 update_last_changed_col: str | None = None,
                 gender_col: str | None = None,
                 autofill_from_users=None,
                 on_error=None,
                 unique_pair_cols: tuple[str, str] | None = None,
                 id_like_cols: list[str] | None = None,
                 versenyid_getter=None,
                 parent=None):
        super().__init__(parent)
        self.df = df.copy()
        for c in columns:
            if c not in self.df.columns:
                self.df[c] = ""
        self.df = self.df[columns]
        self.columns = columns
        self.update_last_changed_col = update_last_changed_col
        self.gender_col = gender_col
        self.autofill_from_users = autofill_from_users
        self.on_error = on_error
        self.unique_pair_cols = unique_pair_cols
        self.id_like_cols = set(id_like_cols or [])
        self.versenyid_getter = versenyid_getter
        # mely oszlopokban keresünk
        self._search_cols_default = ["Versenyengedelyszam", "Name", "Phone number", "Email", "Egyesulet"]
        self._rebuild_search_blob()

    # --------- segéd: ID normalizálás (pl. 123.0 -> 123)
    def _normalize_id_like(self, col_name: str, value) -> str:
        if col_name not in self.id_like_cols:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return ""
            return str(value)
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ""
        if isinstance(value, (int, )):
            return str(value)
        if isinstance(value, float):
            if float(value).is_integer():
                return str(int(value))
            return str(value)
        s = str(value).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    def is_row_empty(self, r: int) -> bool:
        """Igaz, ha a sor minden mezője üres (normalizálva)."""
        if r < 0 or r >= len(self.df):
            return True
        for c, col in enumerate(self.columns):
            v = self.df.iat[r, c]
            s = self._normalize_id_like(col, v).strip()
            if s:
                return False
        return True

    def rowCount(self, parent=QModelIndex()):
        return len(self.df)

    def columnCount(self, parent=QModelIndex()):
        return len(self.columns)

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole):
        if not index.isValid():
            return None
        r, c = index.row(), index.column()
        col_name = self.columns[c]
        value = self.df.iloc[r][col_name] if col_name in self.df.columns else ""
        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            if pd.isna(value):
                return ""
            return self._normalize_id_like(col_name, value)
        return None

    def headerData(self, section, orientation, role=Qt.ItemDataRole.DisplayRole):
        if role != Qt.ItemDataRole.DisplayRole:
            return None
        if orientation == Qt.Orientation.Horizontal:
            return self._wrap_header(self.columns[section])
        return str(section + 1)

    def flags(self, index: QModelIndex):
        if not index.isValid():
            return Qt.ItemFlag.NoItemFlags
        return Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsEditable

    def setData(self, index: QModelIndex, value, role=Qt.ItemDataRole.EditRole):
        if role != Qt.ItemDataRole.EditRole or not index.isValid():
            return False
        r, c = index.row(), index.column()
        col_name = self.columns[c]

        # Bemenet normalizálása
        new_val_raw = "" if value is None else value
        new_val = self._normalize_id_like(col_name, new_val_raw)

        # Gender validáció
        if self.gender_col and col_name == self.gender_col:
            v = new_val.strip()
            if v.lower() == "m":
                new_val = "M"
            elif v.lower() == "f":
                new_val = "F"
            elif v == "":
                new_val = ""
            else:
                if self.on_error:
                    self.on_error("Hibás nem", "A Gender mező csak M=Male vagy F=Female lehet!\nEgyéb gendert a rendszer nem kezel.")
                return False

        prev_val = "" if pd.isna(self.df.iat[r, c]) else self._normalize_id_like(col_name, self.df.iat[r, c])
        if prev_val == new_val:
            return True

        # Duplikáció tiltása (unique (A,B) pár)
        if self.unique_pair_cols is not None and col_name in self.unique_pair_cols:
            a_col, b_col = self.unique_pair_cols
            a_val = new_val if col_name == a_col else self._normalize_id_like(a_col, self.df.at[r, a_col] if a_col in self.df.columns else "")
            b_val = new_val if col_name == b_col else self._normalize_id_like(b_col, self.df.at[r, b_col] if b_col in self.df.columns else "")
            # If either key is empty, skip duplicate check so the user can clear entries.
            if str(a_val).strip() == "" or str(b_val).strip() == "":
                pass  # Skip duplicate check when either value is empty
            else:
                if a_col in self.df.columns and b_col in self.df.columns:
                    mask = (self.df.index != r) & (self.df[a_col].astype(str).map(lambda x: self._normalize_id_like(a_col, x)) == a_val) & \
                           (self.df[b_col].astype(str).map(lambda x: self._normalize_id_like(b_col, x)) == b_val)
                    if mask.any():
                        if self.on_error:
                            self.on_error(
                                "Duplikált páros",
                                f"Ugyanaz a Versenyengedélyszám már szerepel ezen a versenyen (Verseny_ID={a_val})."
                                if a_col == "Verseny_ID" and b_col == "Versenyengedelyszam"
                                else "Ez a kulcspár már létezik."
                            )
                        return False

        # Érték rögzítése
        self.df.iat[r, c] = new_val

        # Autofill az első oszlop alapján
        if self.autofill_from_users and c == 0:
            key = new_val.strip()
            if key:
                try:
                    users_df = self.autofill_from_users()
                except Exception:
                    users_df = pd.DataFrame(columns=["Versenyengedelyszam"])
                    log_error("Autofill felhasználó betöltési hiba", traceback.format_exc())
                match = users_df[users_df["Versenyengedelyszam"].astype(str) == key] if "Versenyengedelyszam" in users_df.columns else pd.DataFrame()
                if not match.empty:
                    user_row = match.iloc[0]
                    for col_idx, col_name2 in enumerate(self.columns):
                        if col_idx == 0:
                            continue
                        if col_name2 in user_row:
                            val2 = "" if pd.isna(user_row[col_name2]) else str(user_row[col_name2])
                            self.df.iat[r, col_idx] = val2
                            top_left = self.index(r, col_idx)
                            self.dataChanged.emit(top_left, top_left, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole])

            # ÚJ: ha van verseny-id getter és a Verseny_ID oszlop létezik,
            # akkor automatikusan töltsük be az aktuális Verseny_ID-t, ha az még üres.
            if self.versenyid_getter and "Verseny_ID" in self.columns:
                try:
                    idx_vid = self.columns.index("Verseny_ID")
                    cur_vid = "" if pd.isna(self.df.iat[r, idx_vid]) else self._normalize_id_like("Verseny_ID", self.df.iat[r, idx_vid]).strip()
                    if not cur_vid:
                        try:
                            vid = self.versenyid_getter()
                        except Exception:
                            vid = ""
                        if vid:
                            self.df.iat[r, idx_vid] = vid
                            top_left = self.index(r, idx_vid)
                            self.dataChanged.emit(top_left, top_left, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole])
                except Exception:
                    # nem kritikus, csak logoljuk
                    log_error("Verseny_ID autofill hiba", traceback.format_exc())

        # Last_changed frissítés
        if self.update_last_changed_col and col_name != self.update_last_changed_col:
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            if self.update_last_changed_col in self.columns:
                idx_lc = self.columns.index(self.update_last_changed_col)
                self.df.iat[r, idx_lc] = now
                top_left = self.index(r, idx_lc)
                self.dataChanged.emit(top_left, top_left, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole])

        # keresési blob frissítés
        try:
            cols = [cname for cname in self._search_cols_default if cname in self.df.columns]
            joined = " ".join("" if pd.isna(self.df.at[r, cname]) else str(self.df.at[r, cname]) for cname in cols).lower()
            for cname in cols:
                if cname in self.id_like_cols:
                    joined = joined.replace(str(self.df.at[r, cname]), self._normalize_id_like(cname, self.df.at[r, cname]))
            self._search_blob.iat[r] = joined
        except Exception:
            log_error("Keresési cache frissítési hiba", traceback.format_exc())

        self.dataChanged.emit(index, index, [Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole])
        return True

    def insertRows(self, row: int, count: int, parent=QModelIndex()):
        if count <= 0:
            return False
        self.beginInsertRows(QModelIndex(), row, row + count - 1)
        for _ in range(count):
            new_row = {col: "" for col in self.columns}
            self.df = pd.concat([self.df.iloc[:row], pd.DataFrame([new_row], columns=self.columns), self.df.iloc[row:]], ignore_index=True)
        self.endInsertRows()
        try:
            for _ in range(count):
                self._search_blob = pd.concat([self._search_blob.iloc[:row], pd.Series([""]), self._search_blob.iloc[row:]], ignore_index=True)
        except Exception:
            self._rebuild_search_blob()
        return True

    def removeRows(self, row: int, count: int, parent=QModelIndex()):
        if count <= 0 or row < 0 or row + count > len(self.df):
            return False
        self.beginRemoveRows(QModelIndex(), row, row + count - 1)
        idx = list(range(row, row + count))
        self.df = self.df.drop(index=idx).reset_index(drop=True)
        try:
            self._search_blob = self._search_blob.drop(index=idx).reset_index(drop=True)
        except Exception:
            self._rebuild_search_blob()
        self.endRemoveRows()
        return True

    def _wrap_header(self, text, max_len=10):
        words, current = [], ""
        for c in text:
            current += c
            if len(current) >= max_len or c in " _":
                words.append(current)
                current = ""
        if current:
            words.append(current)
        return "\n".join(words)

    def _rebuild_search_blob(self):
        try:
            cols = [c for c in self._search_cols_default if c in self.df.columns]
            if cols:
                def norm_series(col):
                    if col in self.id_like_cols:
                        return self.df[col].apply(lambda v: self._normalize_id_like(col, v))
                    return self.df[col].fillna("").astype(str)
                parts = [norm_series(c) for c in cols]
                tmp = pd.concat(parts, axis=1)
                self._search_blob = tmp.fillna("").astype(str).agg(" ".join, axis=1).str.lower()
            else:
                self._search_blob = pd.Series([""] * len(self.df), index=self.df.index)
        except Exception:
            self._search_blob = pd.Series([""] * len(self.df), index=self.df.index)
            log_error("Keresési cache építési hiba", traceback.format_exc())


class SearchFilterProxy(QSortFilterProxyModel):
    """
    Teljes sor-szűrés több oszlop összefűzött blobján.
    - AND logika több szó esetén
    - Kis/nagybetűtől független
    - Sorlimit: csak az első N találatot engedi át (gyors üres keresésnél is)
    - Teljes találatszám követése
    - FIX: az utolsó, üres sor mindig látszódjon
    - FIX: keresés esetén nincs limit, minden találatot mutat
    - PERFORMANCE: 12000+ sorokhoz optimalizált
    """
    def __init__(self, source_model: PandasTableModel, row_limit: int = 500, parent=None):
        super().__init__(parent)
        self._text = ""
        self._row_limit = int(row_limit)
        self._accepted_so_far = 0
        self._matched_total = 0
        self.setSourceModel(source_model)
        self.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)

        sm = self.sourceModel()
        sm.modelReset.connect(self._reset_counter)
        sm.layoutChanged.connect(self._reset_counter)
        sm.dataChanged.connect(self._reset_counter)
        sm.rowsInserted.connect(self._reset_counter)
        sm.rowsRemoved.connect(self._reset_counter)

    def _reset_counter(self, *args, **kwargs):
        self._accepted_so_far = 0
        self._matched_total = 0

    def setRowLimit(self, n: int):
        self._row_limit = max(1, int(n))
        self._reset_counter()
        self.invalidateFilter()

    def currentRowLimit(self) -> int:
        return self._row_limit

    def matchedTotal(self) -> int:
        return int(self._matched_total)

    def setFilterText(self, text: str):
        self._text = (text or "").strip().lower()
        self._reset_counter()
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex):
        model: PandasTableModel = self.sourceModel()  # type: ignore

        # Mindig engedjük át AZ UTOLSÓ üres sort (placeholdert),
        # de ne növelje a találati számlálót és ne számítson limitbe.
        try:
            is_last_row = (source_row == model.rowCount() - 1)
            if is_last_row and model.is_row_empty(source_row):
                return True
        except Exception:
            pass

        # Egyéb üres sorokat rejtsük el
        try:
            if model.is_row_empty(source_row):
                return False
        except Exception:
            return False

        # Ha nincs keresési szöveg (üres keresés), akkor alkalmazzuk a row_limit-et
        if not self._text:
            self._matched_total += 1
            if self._accepted_so_far >= self._row_limit:
                return False
            self._accepted_so_far += 1
            return True

        # Keresési szöveg feldolgozása
        try:
            row_text = model._search_blob.iat[source_row]
        except Exception:
            return False

        terms = [t for t in self._text.split() if t]
        ok = all(t in row_text for t in terms)
        if ok:
            self._matched_total += 1
            
            # TELJESÍTMÉNY OPTIMALIZÁLÁS 12000+ sorokhoz:
            # - 1 karakter: nincs keresés (túl sok találat)
            # - 2 karakter: max 100 találat (gyors)
            # - 3+ karakter: minden eredmény (pontos)
            search_length = len(self._text.strip())
            if search_length == 1:
                # 1 karakter: túl sok találat, ne keressünk
                return False
            elif search_length == 2:
                # 2 karakter: csak az első 100 találatot mutatjuk (gyorsaság)
                if self._accepted_so_far >= 100:
                    return False
            # 3+ karakter esetén nincs limit - minden találatot megjelenítünk
            
            self._accepted_so_far += 1
            return True
        return False


# --------------------------
#   View + logika (Tabs)
# --------------------------

class TableTab(QWidget):
    def __init__(self, load_func, save_func, columns, parent=None, update_last_changed_col=None, gender_col=None,
                 enable_search=False, autofill_from_users=None, versenyid_selector=None,
                 unique_pair_cols: tuple[str, str] | None = None, id_like_cols: list[str] | None = None,
                 allow_sorting: bool = True, show_add_button: bool = True, hide_add_button: bool = False):
        super().__init__(parent)
        self.load_func = load_func
        self.save_func = save_func
        self.columns = columns
        self.update_last_changed_col = update_last_changed_col
        self.gender_col = gender_col
        self.enable_search = enable_search
        self.autofill_from_users = autofill_from_users  # callable returning DataFrame, or None
        self.versenyid_selector = versenyid_selector  # QComboBox vagy None
        self.selected_versenyid = None
        self.unique_pair_cols = unique_pair_cols
        self.id_like_cols = id_like_cols or []
        self.allow_sorting = allow_sorting
        self._show_add_button = show_add_button and not hide_add_button

        # Async save state
        self._save_timer = QTimer(self)
        self._save_timer.setSingleShot(True)
        self._save_timer.timeout.connect(self._do_save_changes)
        self._pending_label = QLabel("")
        self._pending_label.setStyleSheet("color: orange; font-weight: bold;")
        self._pending_label.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
        self._count_label = QLabel("")  # bal alsó számláló
        self._count_label.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)

        self._pending_save = False
        self._save_in_progress = False
        self._thread = None
        self._worker = None
        self._last_error_popup_ts = 0.0

        # Adatok betöltése hibavédelemmel
        try:
            df = self.load_func()
        except Exception:
            log_error("Adatbázis betöltési hiba", traceback.format_exc())
            df = pd.DataFrame(columns=self.columns)
            try:
                QMessageBox.warning(
                    self,
                    "Betöltési hiba",
                    "Nem sikerült betölteni az adatbázist. Üres táblával indul a program.\nRészletek: database/error.log"
                )
            except Exception:
                pass

        layout = QVBoxLayout()
        if self.enable_search:
            search_layout = QHBoxLayout()
            search_label = QLabel("Keresés:")
            self.search_edit = QLineEdit()
            self.search_edit.setPlaceholderText("Írj be keresendő szöveget… (min. 2 karakter)")
            search_layout.addWidget(search_label)
            search_layout.addWidget(self.search_edit)
            layout.addLayout(search_layout)

        # VersenyID selector csak az eredmények tabon (ha van)
        if self.versenyid_selector:
            versenyid_layout = QHBoxLayout()
            versenyid_label = QLabel("Verseny_ID:")
            versenyid_layout.addWidget(versenyid_label)
            versenyid_layout.addWidget(self.versenyid_selector)
            versenyid_layout.addStretch()
            layout.addLayout(versenyid_layout)
            self.versenyid_selector.currentIndexChanged.connect(self.on_versenyid_changed)
            self.selected_versenyid = self.versenyid_selector.currentText()

        # helper getter for model to read current selected verseny id
        def _get_selected_versenyid():
            return self.selected_versenyid or ""

        # Model és Proxy
        self.model = PandasTableModel(
            df, self.columns,
            update_last_changed_col=self.update_last_changed_col,
            gender_col=self.gender_col,
            autofill_from_users=self.autofill_from_users,
            on_error=lambda t, m: QMessageBox.warning(self, t, m),
            unique_pair_cols=self.unique_pair_cols,
            id_like_cols=self.id_like_cols,
            versenyid_getter=_get_selected_versenyid
        )
        self.proxy = SearchFilterProxy(self.model, row_limit=500)

        # Mindig tartsunk egy üres sort a végén
        self._ensure_trailing_empty_row()

        # View
        self.view = QTableView()
        self.view.setModel(self.proxy)
        self.view.setAlternatingRowColors(True)
        self.view.setSortingEnabled(self.allow_sorting)
        if not self.allow_sorting:
            try:
                self.view.horizontalHeader().setSortIndicatorShown(False)
            except Exception:
                pass
        self.view.horizontalHeader().setStretchLastSection(False)
        self.view.horizontalHeader().setDefaultSectionSize(150)
        layout.addWidget(self.view)

        # Gombok
        btn_layout = QHBoxLayout()
        if self._show_add_button:
            add_btn = QPushButton("Új sor hozzáadása")
            add_btn.clicked.connect(self.add_row)
            btn_layout.addWidget(add_btn)

        # "További találatok…" gomb
        self.more_btn = QPushButton("További találatok…")
        def _more():
            self.proxy.setRowLimit(self.proxy.currentRowLimit() + 1000)
            self._update_more_button_visibility()
            self._update_count_label()
            self.view.scrollToBottom()
        self.more_btn.clicked.connect(_more)
        btn_layout.addWidget(self.more_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        # Alsó státusz: balra count, jobbra mentés állapot
        status_layout = QHBoxLayout()
        status_layout.addWidget(self._count_label)
        status_layout.addStretch()
        status_layout.addWidget(self._pending_label)
        layout.addLayout(status_layout)

        self.setLayout(layout)

        # Események
        self.model.dataChanged.connect(self._on_model_data_changed)
        self.model.rowsInserted.connect(lambda *_: (self.schedule_save_changes(), self._update_more_button_visibility(), self._update_count_label()))
        self.model.rowsRemoved.connect(lambda *_: (self.schedule_save_changes(), self._update_more_button_visibility(), self._update_count_label()))

        self.proxy.layoutChanged.connect(lambda *_: (self._update_more_button_visibility(), self._update_count_label()))
        self.proxy.modelReset.connect(lambda *_: (self._update_more_button_visibility(), self._update_count_label()))
        self.proxy.rowsInserted.connect(lambda *_: (self._update_more_button_visibility(), self._update_count_label()))
        self.proxy.rowsRemoved.connect(lambda *_: (self._update_more_button_visibility(), self._update_count_label()))

        if self.enable_search:
            self._search_timer = QTimer(self)
            self._search_timer.setSingleShot(True)
            def _apply_filter():
                self.proxy.setFilterText(self.search_edit.text())
                self._update_more_button_visibility()
                self._update_count_label()
            self._search_timer.timeout.connect(_apply_filter)
            # Increased debounce time for better performance with large datasets
            self.search_edit.textChanged.connect(lambda _: self._search_timer.start(500))

        # induló állapot
        self._update_more_button_visibility()
        self._update_count_label()

    # ===== ÜRES SOR MENEDZSMENT =====
    def _ensure_trailing_empty_row(self):
        """Legyen pontosan 1 üres sor a végén."""
        if self.model.rowCount() == 0:
            self.model.insertRows(0, 1)
            return
        
        # JAVÍTÁS: Távolítsuk el az összes felesleges üres sort a végéről
        # De MINDIG hagyjunk meg legalább egy üres sort a végén
        while self.model.rowCount() >= 2:
            last = self.model.rowCount() - 1
            prev = self.model.rowCount() - 2
            if self.model.is_row_empty(prev) and self.model.is_row_empty(last):
                self.model.removeRows(prev, 1)  # Az előzőt töröljük, az utolsót megtartjuk
            else:
                break
        
        # Biztosítsuk, hogy mindig legyen egy üres sor a végén
        if self.model.rowCount() == 0 or not self.model.is_row_empty(self.model.rowCount() - 1):
            self.model.insertRows(self.model.rowCount(), 1)

    def _on_model_data_changed(self, top_left: QModelIndex, bottom_right: QModelIndex, roles):
        # mentés ütemezés
        self.schedule_save_changes()
        # ha az utolsó sorba írtak -> adjunk új üres sort
        try:
            last = self.model.rowCount() - 1
            if top_left.row() <= last <= bottom_right.row():
                if not self.model.is_row_empty(last):
                    self.model.insertRows(self.model.rowCount(), 1)
        except Exception:
            pass

        # Mindig ellenőrizzük és korrekciózzuk az üres sorokat,
        # így ha valaki elkezdett írni egy sorba majd kitörölte, nem lesz két üres sorunk.
        try:
            self._ensure_trailing_empty_row()
        except Exception:
            pass

        self._update_more_button_visibility()
        self._update_count_label()

    # ===== COUNT LABEL =====
    def _nonempty_total_count(self) -> int:
        n = 0
        for r in range(self.model.rowCount()):
            if not self.model.is_row_empty(r):
                n += 1
        return n

    def _nonempty_visible_count(self) -> int:
        n = 0
        for pr in range(self.proxy.rowCount()):
            src_idx = self.proxy.mapToSource(self.proxy.index(pr, 0))
            if src_idx.isValid() and not self.model.is_row_empty(src_idx.row()):
                n += 1
        return n

    def _update_count_label(self):
        x = self._nonempty_visible_count()
        y = self._nonempty_total_count()
        self._count_label.setText(f"Jelenleg {x} betöltve az {y} (összes) rekordból.")

    # ===== UI gombok & viselkedés =====
    def _update_more_button_visibility(self):
        try:
            total = self.proxy.matchedTotal()
            shown = self.proxy.rowCount()
            self.more_btn.setVisible(shown < total)
        except Exception:
            self.more_btn.setVisible(False)

    def on_versenyid_changed(self, idx):
        if self.versenyid_selector:
            self.selected_versenyid = self.versenyid_selector.currentText()

    def add_row(self):
        insert_at = self.model.rowCount()
        self.model.insertRows(insert_at, 1)
        if self.versenyid_selector and "Verseny_ID" in self.columns:
            idx_col = self.columns.index("Verseny_ID")
            idx = self.model.index(insert_at, idx_col)
            self.model.setData(idx, self.selected_versenyid or "")
        proxy_row = self.proxy.mapFromSource(self.model.index(insert_at, 0)).row()
        if proxy_row >= 0:
            self.view.scrollTo(self.proxy.index(proxy_row, 0))
        self._ensure_trailing_empty_row()
        self._update_more_button_visibility()
        self._update_count_label()

    # ---- Mentési logika
    def schedule_save_changes(self):
        if self._save_timer.isActive() or self._save_in_progress:
            self._pending_save = True
            self._pending_label.setText("Mentésre vár…")
        else:
            self._pending_save = False
            self._pending_label.setText("")
            self._save_timer.start(10000)  # 10s

    def _do_save_changes(self):
        self.save_changes()

    def _snapshot_df(self) -> pd.DataFrame:
        """Mentéshez: csak a nem-üres sorok kerüljenek a fájlba."""
        df = self.model.df.copy()
        mask_nonempty = []
        for r in range(len(df)):
            row_empty = True
            for c, col in enumerate(self.columns):
                v = df.iat[r, c]
                s = "" if pd.isna(v) else str(v).strip()
                if s:
                    row_empty = False
                    break
            mask_nonempty.append(not row_empty)
        if mask_nonempty:
            df = df[pd.Series(mask_nonempty)].reset_index(drop=True)
        return df

    def _start_async_save(self, df):
        if self._save_in_progress:
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
            self._pending_label.setStyleSheet("color: #4CAF50; font-weight: bold;")
            self._pending_label.setText("Mentve")
            QTimer.singleShot(2000, lambda: self._pending_label.setText(""))
            QTimer.singleShot(1, lambda: self._pending_label.setStyleSheet("color: orange; font-weight: bold;"))
            if self._pending_save:
                self._pending_save = False
                self._save_timer.start(2000)
        else:
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
        df_new = self._snapshot_df()
        self._start_async_save(df_new)

    def flush_and_wait(self, timeout_ms=5000):
        """Bezáráskor szinkron mentés, hogy adatvesztést elkerüljük."""
        try:
            if self._save_timer.isActive():
                self._save_timer.stop()
            t0 = time.time()
            while self._save_in_progress and (time.time() - t0) < (timeout_ms / 1000.0):
                QApplication.processEvents()
                time.sleep(0.05)
            df_new = self._snapshot_df()
            try:
                self.save_func(df_new)
            except Exception:
                log_error("Záráskor szinkron mentési hiba", traceback.format_exc())
                _write_recovery_csv(df_new, _target_path_for_save_func(self.save_func))
        except Exception:
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

        # Felhasználók tab: NEM rendezhető, automatikus üres sor, nincs "Új sor" gomb
        self.users_tab = TableTab(
            load_db, save_db, COLUMNS,
            update_last_changed_col="Last_changed",
            gender_col="Gender",
            enable_search=True,
            unique_pair_cols=None,
            id_like_cols=[],
            allow_sorting=False,
            show_add_button=False,
            hide_add_button=True
        )
        tabs.addTab(self.users_tab, "Felhasználók")

        # Eredmények tab: NEM rendezhető, automatikus üres sor, nincs "Új sor" gomb
        if "Kategoria" not in EREDMENY_COLUMNS:
            EREDMENY_COLUMNS.append("Kategoria")
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
            versenyid_selector=versenyid_selector,
            unique_pair_cols=("Verseny_ID", "Versenyengedelyszam"),
            id_like_cols=["Versenyengedelyszam"],
            allow_sorting=False,
            show_add_button=False,
            hide_add_button=True
        )
        tabs.addTab(self.eredmeny_tab, "Eredmények")

        # Versenyek tab: rendezés maradhat, gomb maradhat
        self.versenyek_tab = TableTab(
            load_versenyek_db, save_versenyek_db, VERSENYEK_COLUMNS,
            enable_search=False,
            unique_pair_cols=None,
            id_like_cols=[],
            allow_sorting=True,
            show_add_button=True
        )
        tabs.addTab(self.versenyek_tab, "Versenyek")

        layout.addWidget(tabs)
        self.setLayout(layout)

    def closeEvent(self, event):
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
