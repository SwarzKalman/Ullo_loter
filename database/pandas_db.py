import pandas as pd
import os
import shutil
import tempfile
import time
from datetime import datetime

COLUMNS = [
    "Versenyengedelyszam",
    "Name",
    "Egyesulet",
    "Gender",
    "Birth",
    "Phone number",
    "Email",
    "Last_changed",
    "Comment"
]
DB_DIR = "database"
DB_FILE = os.path.join(DB_DIR, "userDB.xlsx")

# ÚJ: Verseny eredmények adatbázis oszlopai és elérési útja
EREDMENY_COLUMNS = [
    "Versenyengedelyszam",
    "Name",
    "Egyesulet",
    "Gender",
    "Birth",
    "Phone number",
    "Email",
    "Last_changed",
    "Comment",
    "KKPI_NY",
    "KKPI_O",
    "NKPI_NY",
    "NKPI_O",
    "KKPU_NY",
    "KKPU_O",
    "NKOU_NY",
    "NKPU_O",
    "SORET_NY",
    "Soret_O",
    "HUZAGOLT_SORET_NY",
    "HUZAGOLT_SORET_O",
    "Verseny_ID"
]
EREDMENY_DB_FILE = os.path.join(DB_DIR, "versenyEredmenyek.xlsx")

VERSENYEK_COLUMNS = [
    "Verseny_ID",
    "Verseny_start",
    "Verseny_end",
    "Szervezo"
]
VERSENYEK_DB_FILE = os.path.join(DB_DIR, "versenyekDB.xlsx")

def _ensure_dir():
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR, exist_ok=True)

def _atomic_write_excel(path, df, retries=7, base_delay=0.3):
    """
    Excel írás atomikusan, több próbálkozással (Excel általi fájlzár esetén is).
    - Ideiglenes fájlba írunk, majd os.replace ugyanarra a fájlrendszerre.
    - Ha PermissionError vagy más hiba történik, exponenciális visszavárakozással próbáljuk újra.
    """
    _ensure_dir()
    dir_name = os.path.dirname(path) or "."
    last_exc = None
    for attempt in range(retries):
        fd, tmp_path = tempfile.mkstemp(prefix=os.path.basename(path) + ".", suffix=".tmp.xlsx", dir=dir_name)
        os.close(fd)
        try:
            df.to_excel(tmp_path, index=False, engine="openpyxl")
            try:
                os.replace(tmp_path, path)  # atomic on same filesystem
                return
            except Exception as e:
                last_exc = e
        except Exception as e:
            last_exc = e
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass
        if attempt < retries - 1:
            time.sleep(base_delay * (2 ** attempt))
    if last_exc:
        raise last_exc
    raise RuntimeError("Ismeretlen hiba történt az Excel atomikus mentése közben.")

def _backup_file(path):
    if os.path.exists(path):
        bak_path = path + ".bak"
        try:
            shutil.copy2(path, bak_path)
        except Exception:
            # ignore backup errors, continue
            pass

def _safe_read_excel(path, fallback_columns):
    try:
        return pd.read_excel(path, engine="openpyxl")
    except Exception:
        # try backup
        bak_path = path + ".bak"
        try:
            if os.path.exists(bak_path):
                return pd.read_excel(bak_path, engine="openpyxl")
        except Exception:
            pass
        # fallback to empty df with expected columns
        return pd.DataFrame(columns=fallback_columns)

def _ensure_columns_and_order(df, required_columns):
    """
    Gondoskodik arról, hogy az elvárt oszlopok meglegyenek,
    és a mentésnél az elvárt oszlopok kerüljenek előre, a maradék oszlopok utánuk.
    """
    df_out = df.copy()
    for col in required_columns:
        if col not in df_out.columns:
            df_out[col] = ""
    # rendelés: required + extra
    extras = [c for c in df_out.columns if c not in required_columns]
    ordered = list(required_columns) + extras
    return df_out[ordered]

def load_db():
    _ensure_dir()
    if not os.path.exists(DB_FILE):
        df = pd.DataFrame(columns=COLUMNS)
        _atomic_write_excel(DB_FILE, df)
    df = _safe_read_excel(DB_FILE, COLUMNS)
    # Oszlopok átnevezése és hozzáadása, ha szükséges
    if "MDLSZ_ID" in df.columns:
        df = df.rename(columns={"MDLSZ_ID": "Versenyengedelyszam"})
    if "ID" in df.columns:
        df = df.rename(columns={"ID": "Versenyengedelyszam"})
    if "Egyesület" in df.columns:
        df = df.rename(columns={"Egyesület": "Egyesulet"})
    # Ensure required columns exist
    for col in COLUMNS:
        if col not in df.columns:
            if "Name" in df.columns:
                insert_pos = df.columns.get_loc("Name") + 1
                df.insert(insert_pos, col, "")
            else:
                df[col] = ""
    # Normalize Versenyengedelyszam to string to make merges predictable
    df["Versenyengedelyszam"] = df["Versenyengedelyszam"].fillna("").astype(str)
    # Ensure Comment exists
    if "Comment" not in df.columns:
        df["Comment"] = ""
    return df

def save_db(df):
    _ensure_dir()
    _backup_file(DB_FILE)
    df_to_save = _ensure_columns_and_order(df, COLUMNS)
    _atomic_write_excel(DB_FILE, df_to_save)

def add_entry(df, name, egyesulet, gender, birth, phone, email, comment=""):
    # Generate a numeric new id robustly even if existing IDs are strings
    if "Versenyengedelyszam" in df.columns and not df["Versenyengedelyszam"].astype(str).replace("", "0").empty:
        # coerce to numeric, ignore non-numeric
        nums = pd.to_numeric(df["Versenyengedelyszam"], errors='coerce').fillna(0).astype(int)
        new_id = int(nums.max()) + 1 if not nums.empty else 1
    else:
        new_id = 1
    now = datetime.now().isoformat()
    new_row = {
        "Versenyengedelyszam": new_id,
        "Name": name,
        "Egyesulet": egyesulet,
        "Gender": gender,
        "Birth": birth,
        "Phone number": phone,
        "Email": email,
        "Last_changed": now,
        "Comment": comment
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_db(df)
    return df

# ÚJ: Verseny eredmények adatbázis kezelése
def load_eredmeny_db():
    _ensure_dir()
    if not os.path.exists(EREDMENY_DB_FILE):
        df = pd.DataFrame(columns=EREDMENY_COLUMNS)
        _atomic_write_excel(EREDMENY_DB_FILE, df)
        return df
    df = _safe_read_excel(EREDMENY_DB_FILE, EREDMENY_COLUMNS)
    # Oszlopok hozzáadása, ha hiányzik valamelyik
    for col in EREDMENY_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df

def save_eredmeny_db(df):
    _ensure_dir()
    _backup_file(EREDMENY_DB_FILE)
    df_to_save = _ensure_columns_and_order(df, EREDMENY_COLUMNS)
    _atomic_write_excel(EREDMENY_DB_FILE, df_to_save)

# ÚJ: Versenyek adatbázis kezelése
def load_versenyek_db():
    _ensure_dir()
    if not os.path.exists(VERSENYEK_DB_FILE):
        df = pd.DataFrame(columns=VERSENYEK_COLUMNS)
        _atomic_write_excel(VERSENYEK_DB_FILE, df)
        return df
    df = _safe_read_excel(VERSENYEK_DB_FILE, VERSENYEK_COLUMNS)
    for col in VERSENYEK_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df

def save_versenyek_db(df):
    _ensure_dir()
    _backup_file(VERSENYEK_DB_FILE)
    df_to_save = _ensure_columns_and_order(df, VERSENYEK_COLUMNS)
    _atomic_write_excel(VERSENYEK_DB_FILE, df_to_save)

# Példa használat:
if __name__ == "__main__":
    df = load_db()
    print(df)
    eredmeny_df = load_eredmeny_db()
    print(eredmeny_df)
    versenyek_df = load_versenyek_db()
    print(versenyek_df)
