mimport pandas as pd
import os
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
        os.makedirs(DB_DIR)

def load_db():
    _ensure_dir()
    if not os.path.exists(DB_FILE):
        df = pd.DataFrame(columns=COLUMNS)
        df.to_excel(DB_FILE, index=False)
    else:
        df = pd.read_excel(DB_FILE)
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
                # try to insert after Name if possible
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
    # Save a backup before overwriting to avoid accidental data loss
    if os.path.exists(DB_FILE):
        try:
            bak = DB_FILE + ".bak"
            df_existing = pd.read_excel(DB_FILE)
            df_existing.to_excel(bak, index=False)
        except Exception:
            # ignore backup errors
            pass
    df.to_excel(DB_FILE, index=False)

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
        df.to_excel(EREDMENY_DB_FILE, index=False)
    else:
        df = pd.read_excel(EREDMENY_DB_FILE)
        # Oszlopok hozzáadása, ha hiányzik valamelyik
        for col in EREDMENY_COLUMNS:
            if col not in df.columns:
                df[col] = ""
    return df

def save_eredmeny_db(df):
    _ensure_dir()
    df.to_excel(EREDMENY_DB_FILE, index=False)

# ÚJ: Versenyek adatbázis kezelése
def load_versenyek_db():
    _ensure_dir()
    if not os.path.exists(VERSENYEK_DB_FILE):
        df = pd.DataFrame(columns=VERSENYEK_COLUMNS)
        df.to_excel(VERSENYEK_DB_FILE, index=False)
    else:
        df = pd.read_excel(VERSENYEK_DB_FILE)
        for col in VERSENYEK_COLUMNS:
            if col not in df.columns:
                df[col] = ""
    return df

def save_versenyek_db(df):
    _ensure_dir()
    df.to_excel(VERSENYEK_DB_FILE, index=False)

# Példa használat:
if __name__ == "__main__":
    df = load_db()
    print(df)
    eredmeny_df = load_eredmeny_db()
    print(eredmeny_df)
    versenyek_df = load_versenyek_db()
    print(versenyek_df)
