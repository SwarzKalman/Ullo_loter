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
DB_FILE = os.path.join(DB_DIR, "data.xlsx")

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
        # Ha ékezetes "Egyesület" van, nevezzük át
        if "Egyesület" in df.columns:
            df = df.rename(columns={"Egyesület": "Egyesulet"})
        # Csak akkor szúrjuk be, ha tényleg nincs ilyen oszlop
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

# Példa használat:
if __name__ == "__main__":
    df = load_db()
    #df = add_entry(df, "Teszt Elek", "Teszt Egyesület", "M", "1990-01-01", "+3612345678", "teszt@valami.hu", "Megjegyzés")
    print(df)
