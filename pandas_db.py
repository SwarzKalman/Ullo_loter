import pandas as pd
import os
from datetime import datetime

DB_FILE = "data.xlsx"
COLUMNS = ["ID", "Name", "Gender", "Birth", "Phone number", "Email", "Last_changed"]

def load_db():
    if not os.path.exists(DB_FILE):
        df = pd.DataFrame(columns=COLUMNS)
        df.to_excel(DB_FILE, index=False)
    else:
        df = pd.read_excel(DB_FILE)
    return df

def save_db(df):
    df.to_excel(DB_FILE, index=False)

def add_entry(df, name, gender, birth, phone, email):
    new_id = (df["ID"].max() + 1) if not df.empty else 1
    now = datetime.now().isoformat()
    new_row = {
        "ID": new_id,
        "Name": name,
        "Gender": gender,
        "Birth": birth,
        "Phone number": phone,
        "Email": email,
        "Last_changed": now
    }
    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    save_db(df)
    return df

# Példa használat:
if __name__ == "__main__":
    df = load_db()
    # df = add_entry(df, "Teszt Elek", "M", "1990-01-01", "+3612345678", "teszt@valami.hu")
    print(df)
