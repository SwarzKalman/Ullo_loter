#!/usr/bin/env python3
"""
presentation.py

Fullscreen projector view that cycles through result categories showing rankings
(name + score) for the selected Verseny_ID. Data is reloaded every REFRESH_SECONDS
(180s by default) because scores are updated continuously. The visible category
changes every CYCLE_SECONDS (10s by default).

Usage:
    python presentation.py                       # picks latest Verseny_ID from DB
    python presentation.py --verseny VID_00001    # show specific Verseny_ID
    python presentation.py --refresh 120 --cycle 8
"""
import argparse
import time
from datetime import datetime
import threading

try:
    import tkinter as tk
    from tkinter import font as tkfont
    from tkinter import ttk
except Exception as e:
    raise RuntimeError("Tkinter is required to run this presentation script.") from e

import pandas as pd

# Import database helpers from existing module
from database.pandas_db import load_eredmeny_db, load_versenyek_db

# Categories to cycle through (order matters)
CATEGORIES = [
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
    "HUZAGOLT_SORET_O"
]

DEFAULT_CYCLE_SECONDS = 10
DEFAULT_REFRESH_SECONDS = 180  # 3 minutes

class PresentationApp:
    def __init__(self, verseny_id: str | None = None, cycle_seconds: int = DEFAULT_CYCLE_SECONDS, refresh_seconds: int = DEFAULT_REFRESH_SECONDS, max_rows: int = 20):
        self.verseny_id = verseny_id or ""
        self.cycle_seconds = int(cycle_seconds)
        self.refresh_seconds = int(refresh_seconds)
        self.max_rows = int(max_rows)

        self.eredmeny_df = pd.DataFrame()
        self.versenyek_df = pd.DataFrame()
        self.rankings = {}  # category -> DataFrame sorted

        # UI state
        self.cat_index = 0
        self.last_refresh_ts: float | None = None

        # Build UI
        self.root = tk.Tk()
        self.root.title("Presentation")
        # Fullscreen suitable for projectors
        # Start windowed so the window can be dragged to another monitor.
        # Fullscreen can be toggled with F11 (see binding below).
        self.fullscreen = False
        try:
            self.root.geometry("1280x720")
        except Exception:
            pass

        self.bg_color = "#000000"
        self.fg_color = "#FFFFFF"
        self.root.configure(bg=self.bg_color)

        # Layout: Title area, timestamp, list area
        self.title_font = tkfont.Font(family="Helvetica", size=48, weight="bold")
        self.item_font = tkfont.Font(family="Helvetica", size=36)
        self.meta_font = tkfont.Font(family="Helvetica", size=18)

        self.title_label = tk.Label(self.root, text="", fg=self.fg_color, bg=self.bg_color, font=self.title_font)
        self.title_label.pack(pady=(40, 10))

        self.list_frame = tk.Frame(self.root, bg=self.bg_color)
        self.list_frame.pack(expand=True, fill=tk.BOTH, padx=80)

        # Pre-create rows to speed rendering
        self.row_labels = []
        for i in range(self.max_rows):
            lbl = tk.Label(self.list_frame, text="", anchor="w", fg=self.fg_color, bg=self.bg_color, font=self.item_font)
            lbl.pack(fill=tk.X, pady=4)
            self.row_labels.append(lbl)

        self.meta_label = tk.Label(self.root, text="", fg=self.fg_color, bg=self.bg_color, font=self.meta_font)
        self.meta_label.pack(pady=(0, 40))
 
        # key bindings: Escape to quit and F11 to toggle fullscreen
        self.root.bind("<Escape>", lambda e: self.root.quit())
        self.root.bind("<F11>", self._on_toggle_fullscreen)

        # Start background tasks
        self._stop_event = threading.Event()
        # First load synchronously so UI doesn't show empty at start
        self._do_refresh()
        # Start scheduled refresh and cycle using Tk's after loop
        self._schedule_refresh()
        self._schedule_cycle()

    def _on_toggle_fullscreen(self, event=None):
        """Toggle fullscreen/windowed state. Bound to F11."""
        # Ensure attribute exists
        self.fullscreen = not getattr(self, "fullscreen", False)
        try:
            # Prefer attributes (works on many platforms)
            self.root.attributes("-fullscreen", self.fullscreen)
        except Exception:
            # Fallback to window state changes
            try:
                if self.fullscreen:
                    self.root.state("zoomed")
                else:
                    self.root.state("normal")
                    try:
                        self.root.geometry("1280x720")
                    except Exception:
                        pass
            except Exception:
                pass

    # ---- Data handling ----
    def _load_dbs(self):
        try:
            self.eredmeny_df = load_eredmeny_db()
        except Exception:
            self.eredmeny_df = pd.DataFrame()
        try:
            self.versenyek_df = load_versenyek_db()
        except Exception:
            self.versenyek_df = pd.DataFrame()

    def _pick_latest_verseny(self) -> str | None:
        # Choose latest non-empty Verseny_ID from versenyek DB if verseny_id not provided
        if self.verseny_id:
            return self.verseny_id
        if self.versenyek_df is None or self.versenyek_df.empty:
            return None
        if "Verseny_ID" not in self.versenyek_df.columns:
            return None
        vals = self.versenyek_df["Verseny_ID"].dropna().astype(str).str.strip()
        vals = [v for v in vals if v]
        if not vals:
            return None
        # Prefer the last row's Verseny_ID (assumes append order)
        return str(vals.iloc[-1]) if hasattr(vals, "iloc") else vals[-1]

    def _build_rankings(self):
        # Build rankings for the currently selected verseny_id
        df = self.eredmeny_df.copy() if self.eredmeny_df is not None else pd.DataFrame()
        if df.empty:
            self.rankings = {}
            return
        # Filter by Verseny_ID if present
        selected_vid = self._pick_latest_verseny() or ""
        if self.verseny_id:
            selected_vid = self.verseny_id
        # store selected for display
        self.selected_vid = selected_vid or ""
        if selected_vid:
            if "Verseny_ID" in df.columns:
                df = df[df["Verseny_ID"].astype(str).str.strip() == str(selected_vid).strip()]
            else:
                # no Verseny_ID column: nothing to show
                df = df.iloc[0:0]

        rankings = {}
        for cat in CATEGORIES:
            if cat not in df.columns:
                # if category missing, create empty ranking
                rankings[cat] = pd.DataFrame(columns=["Name", "Score"])
                continue
            # Special-case: Comment column should display textual comments (not numeric scores)
            if cat == "Comment":
                names = df["Name"] if "Name" in df.columns else pd.Series([""] * len(df), index=df.index)
                comments = df["Comment"].astype(str).fillna("") if "Comment" in df.columns else pd.Series([""] * len(df), index=df.index)
                tmp = pd.DataFrame({"Name": names.astype(str).fillna(""), "Text": comments})
                # Exclude empty comments from the board
                try:
                    tmp = tmp[tmp["Text"].str.strip() != ""].reset_index(drop=True)
                except Exception:
                    pass
                rankings[cat] = tmp
                continue
            # Coerce score to numeric (non-numeric -> NaN -> fill 0)
            try:
                scores = pd.to_numeric(df[cat], errors="coerce").fillna(0)
            except Exception:
                # If column has complex values, fallback to 0
                scores = pd.Series([0] * len(df), index=df.index)
            names = df["Name"] if "Name" in df.columns else pd.Series([""] * len(df), index=df.index)
            tmp = pd.DataFrame({"Name": names.astype(str).fillna(""), "Score": scores})
            tmp_sorted = tmp.sort_values(by="Score", ascending=False, kind="mergesort").reset_index(drop=True)
            # Exclude competitors with zero or negative score from the presentation board
            try:
                tmp_sorted = tmp_sorted[tmp_sorted["Score"] > 0].reset_index(drop=True)
            except Exception:
                # In case Score column is not comparable, fall back to original sorted frame
                pass
            rankings[cat] = tmp_sorted
        self.rankings = rankings

    # ---- UI update ----
    def _render_category(self, cat_name: str):
        # Title: category name and selected verseny id
        title_text = f"{cat_name}"
        if self.selected_vid:
            title_text += f" — {self.selected_vid}"
        self.title_label.config(text=title_text)

        df = self.rankings.get(cat_name, pd.DataFrame())
        # Show top rows up to max_rows
        for i in range(self.max_rows):
            if i < len(df):
                row = df.iloc[i]
                name = str(row.get("Name", "")).strip()
                # If this is the Comment view, display the comment text instead of a numeric score
                if cat_name == "Comment":
                    comment_text = str(row.get("Text", "")).strip()
                    display = f"{i+1:2d}. {name} — {comment_text}"
                else:
                    score = row.get("Score", 0)
                    display = f"{i+1:2d}. {name} — {score:.2f}" if isinstance(score, (int, float)) else f"{i+1:2d}. {name} — {score}"
                self.row_labels[i].config(text=display)
            else:
                self.row_labels[i].config(text="")

        # Meta: last refresh
        ts = datetime.fromtimestamp(self.last_refresh_ts) if self.last_refresh_ts else None
        ts_text = ts.strftime("%Y-%m-%d %H:%M:%S") if ts else "N/A"
        meta = f"Frissítve: {ts_text}    (Frissítés: {self.refresh_seconds}s, Váltás: {self.cycle_seconds}s)    Nyomj Escape-t a kilépéshez"
        self.meta_label.config(text=meta)

    # ---- Scheduling ----
    def _do_refresh(self):
        # Load DBs and rebuild rankings
        self._load_dbs()
        # If verseny_id not set, attempt pick latest
        if not self.verseny_id:
            picked = self._pick_latest_verseny()
            if picked:
                self.selected_vid = picked
            else:
                self.selected_vid = ""
        else:
            self.selected_vid = self.verseny_id
        self._build_rankings()
        self.last_refresh_ts = time.time()
        # Immediately refresh current visible category
        current_cat = CATEGORIES[self.cat_index % len(CATEGORIES)]
        self._render_category(current_cat)

    def _schedule_refresh(self):
        # Use tkinter's after for safe UI-thread scheduling
        def _periodic():
            if self._stop_event.is_set():
                return
            try:
                self._do_refresh()
            except Exception:
                # swallow exceptions to keep loop alive
                pass
            # schedule next
            try:
                self.root.after(int(self.refresh_seconds * 1000), _periodic)
            except Exception:
                pass
        # schedule first future refresh
        try:
            self.root.after(int(self.refresh_seconds * 1000), _periodic)
        except Exception:
            pass

    def _schedule_cycle(self):
        def _next_cat():
            if self._stop_event.is_set():
                return
            try:
                self.cat_index = (self.cat_index + 1) % len(CATEGORIES)
                current_cat = CATEGORIES[self.cat_index]
                # render using possibly updated rankings
                self._render_category(current_cat)
            except Exception:
                pass
            try:
                self.root.after(int(self.cycle_seconds * 1000), _next_cat)
            except Exception:
                pass
        # render current immediately
        try:
            self._render_category(CATEGORIES[self.cat_index])
        except Exception:
            pass
        try:
            self.root.after(int(self.cycle_seconds * 1000), _next_cat)
        except Exception:
            pass

    def run(self):
        try:
            self.root.mainloop()
        finally:
            self._stop_event.set()

def select_verseny_gui(initial_selection: str | None = None) -> str | None:
    """
    Show a small window with a combobox listing available Verseny_ID values.
    Returns the selected Verseny_ID string, or None if cancelled.
    """
    try:
        vdf = load_versenyek_db()
    except Exception:
        vdf = pd.DataFrame()

    items = []
    if vdf is not None and not vdf.empty and "Verseny_ID" in vdf.columns:
        items = [str(v).strip() for v in vdf["Verseny_ID"].dropna().unique() if str(v).strip()]

    result = {"value": None}

    sel_root = tk.Tk()
    sel_root.title("Válassz Verseny_ID-t")
    sel_root.geometry("700x180")
    sel_root.configure(bg="#222222")
    try:
        sel_root.attributes("-topmost", True)
    except Exception:
        pass

    lbl = tk.Label(sel_root, text="Válaszd ki a megjelenítendő Verseny_ID-t:", fg="#FFFFFF", bg="#222222", font=tkfont.Font(size=14))
    lbl.pack(pady=(12, 6))

    combo_var = tk.StringVar()
    combo = ttk.Combobox(sel_root, textvariable=combo_var, values=items, font=tkfont.Font(size=14), width=60)
    combo.pack(pady=(0, 8))
    if initial_selection:
        combo_var.set(initial_selection)
    elif items:
        combo_var.set(items[-1])

    info_lbl = tk.Label(sel_root, text="Ha nincs a listában, beírhatsz kézzel egy Verseny_ID-t.", fg="#CCCCCC", bg="#222222", font=tkfont.Font(size=10))
    info_lbl.pack(pady=(0, 8))

    btn_frame = tk.Frame(sel_root, bg="#222222")
    btn_frame.pack(pady=(6, 6))

    def on_start():
        val = combo_var.get().strip()
        if val == "":
            # user didn't choose anything — treat as cancel
            result["value"] = None
        else:
            result["value"] = val
        sel_root.destroy()

    def on_cancel():
        result["value"] = None
        sel_root.destroy()

    def on_refresh():
        try:
            vdf2 = load_versenyek_db()
            items2 = []
            if vdf2 is not None and not vdf2.empty and "Verseny_ID" in vdf2.columns:
                items2 = [str(v).strip() for v in vdf2["Verseny_ID"].dropna().unique() if str(v).strip()]
            combo["values"] = items2
            if items2:
                combo_var.set(items2[-1])
        except Exception:
            pass

    start_btn = tk.Button(btn_frame, text="Start", command=on_start, width=12)
    start_btn.pack(side=tk.LEFT, padx=8)
    refresh_btn = tk.Button(btn_frame, text="Refresh list", command=on_refresh, width=12)
    refresh_btn.pack(side=tk.LEFT, padx=8)
    cancel_btn = tk.Button(btn_frame, text="Cancel", command=on_cancel, width=12)
    cancel_btn.pack(side=tk.LEFT, padx=8)

    sel_root.mainloop()
    return result["value"]


def parse_args():
    p = argparse.ArgumentParser(description="Presentation screen for competition results.")
    p.add_argument("--verseny", "-v", help="Verseny_ID to show (e.g. VID_00001). If omitted, a selection dialog will open.", default=None)
    p.add_argument("--cycle", "-c", type=int, help=f"Seconds between category switches (default {DEFAULT_CYCLE_SECONDS})", default=DEFAULT_CYCLE_SECONDS)
    p.add_argument("--refresh", "-r", type=int, help=f"Seconds between DB refresh (default {DEFAULT_REFRESH_SECONDS})", default=DEFAULT_REFRESH_SECONDS)
    p.add_argument("--rows", type=int, help="Number of rows to display (default 20)", default=20)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    chosen = args.verseny
    if not chosen:
        # show selection dialog to pick Verseny_ID
        try:
            chosen = select_verseny_gui()
        except Exception:
            chosen = None
    if not chosen:
        import sys
        print("No Verseny_ID selected. Exiting.")
        sys.exit(0)
    app = PresentationApp(verseny_id=chosen, cycle_seconds=args.cycle, refresh_seconds=args.refresh, max_rows=args.rows)
    app.run()