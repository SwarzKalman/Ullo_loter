import pandas as pd
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

def load_db():
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
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
        if "Egyesulet" not in df.columns:
            df.insert(df.columns.get_loc("Name") + 1, "Egyesulet", "")
        if "Comment" not in df.columns:
            df["Comment"] = ""
    return df

def save_db(df):
    df.to_excel(DB_FILE, index=False)

def add_entry(df, name, egyesulet, gender, birth, phone, email, comment=""):
    new_id = (df["Versenyengedelyszam"].max() + 1) if not df.empty else 1
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
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
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
    df.to_excel(EREDMENY_DB_FILE, index=False)

# ÚJ: Versenyek adatbázis kezelése
def load_versenyek_db():
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
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
    df.to_excel(VERSENYEK_DB_FILE, index=False)

# Példa használat:
if __name__ == "__main__":
    df = load_db()
    #df = add_entry(df, "Teszt Elek", "Teszt Egyesület", "M", "1990-01-01", "+3612345678", "teszt@valami.hu", "Megjegyzés")
    print(df)
    eredmeny_df = load_eredmeny_db()
    print(eredmeny_df)
    versenyek_df = load_versenyek_db()
    print(versenyek_df)
