#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import webbrowser
from dataclasses import dataclass
from typing import List, Dict, Any, Tuple, Optional
import threading

# C 레벨 STDERR 일시 무음 컨텍스트
def _silence_stderr_c():
    class _Ctx:
        def __enter__(self):
            import os
            self._devnull = open(os.devnull, "w")
            self._old_stderr_fd = os.dup(2)
            os.dup2(self._devnull.fileno(), 2)
            return self
        def __exit__(self, *exc):
            import os
            os.dup2(self._old_stderr_fd, 2)
            os.close(self._old_stderr_fd)
            self._devnull.close()
    return _Ctx()


# ---- gRPC/absl 잡로그 꺼버리기 ----
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")   # gRPC 로그 최소화
os.environ.setdefault("GLOG_minloglevel", "2")     # absl/glog info, warning 숨김
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2") # 텐서/CPP 로그 줄이기(있으면)

# 외부 패키지
import requests
from bs4 import BeautifulSoup
import google.generativeai as genai

# ---- absl 로거 레벨 내리기 ----
try:
    from absl import logging as absl_logging
    absl_logging.set_verbosity(absl_logging.ERROR)
except Exception:
    pass

# ---------------- Config ----------------
DEFAULT_KR_PATH = r"D:\2025_currently_working\식품안전1팀\KR_DB_0925.xlsx"
DEFAULT_US_PATH = r"D:\2025_currently_working\식품안전1팀\US_DB_0926.xlsx"
# ▶ EU 기본 경로(임시). 실제 파일 경로로 교체하거나, 실행 후 '찾기...'로 선택하세요.
DEFAULT_EU_PATH = r"(시연용) EU_Additives.xlsx"

TRUNCATE_AT = 80
ROW_ID_COL = "__ROW_ID__"   # 트리 내부용 숨김 인덱스 컬럼

# --- Gemini API Configuration ---
ENV_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()
HARDCODED_KEY = "AIzaSyDpPvneo1OyY2a6DUZHgSOWdpcbt9rVx4g"  # 필요 시만 키 문자열 입력(없으면 빈 문자열 유지)
API_KEY = ENV_KEY if ENV_KEY else HARDCODED_KEY

GEMINI_CONFIGURED = False
GEMINI_MODEL = None
CHAT_SESSION = None

if API_KEY:
    try:
        # ↓↓↓ 이 줄~아래 3줄을 "with _silence_stderr_c()"로 감싼다
        with _silence_stderr_c():
            genai.configure(api_key=API_KEY)
            GEMINI_MODEL = genai.GenerativeModel("gemini-2.5-flash")
            CHAT_SESSION = GEMINI_MODEL.start_chat(history=[])
        GEMINI_CONFIGURED = True
    except Exception as e:
        print(f"[Gemini] API 초기화 실패: {e}")
        GEMINI_CONFIGURED = False
else:
    print("[Gemini] API 키가 없습니다. 환경변수 GOOGLE_API_KEY 를 설정하거나 HARDCODED_KEY 를 지정하세요.")

# --- Optional dependency handling ---
_MISSING_DEPS = []
try:
    import pandas as pd
except Exception:
    _MISSING_DEPS.append("pandas")

# Prefer RapidFuzz; fallback to difflib
_USE_RAPIDFUZZ = False
try:
    from rapidfuzz import fuzz, process
    _USE_RAPIDFUZZ = True
except Exception:
    from difflib import SequenceMatcher

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter import font as tkfont 

# ----------------- Utilities -----------------
URL_REGEX = re.compile(r"(https?://[^\s,{}()]+)", re.IGNORECASE)
EU_DOMAIN = "ec.europa.eu"

# 공용 CAS 정규식
CAS_PATTERN = re.compile(r"\b\d{2,7}-\d{2}-\d\b")

def open_url(url: str):
    try:
        webbrowser.open(url)
    except Exception as e:
        messagebox.showerror("Link Error", f"Unable to open link:\n{url}\n\n{e}")

def extract_urls(text: str) -> List[str]:
    if not isinstance(text, str):
        return []
    return URL_REGEX.findall(text)

def normalize(s: Any) -> str:
    if s is None:
        return ""
    s = str(s)
    return s.strip()

def normalize_cas(s: Any) -> str:
    return normalize(s)

def lower(s: str) -> str:
    return s.lower()

def tokenize_other_names(s: Any) -> List[str]:
    """세미콜론(;)로 분할하여 동의어/다값 필드 처리."""
    if not isinstance(s, str):
        return []
    parts = [p.strip() for p in re.split(r"[;]", s) if p.strip()]
    return parts

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip() if isinstance(s, str) else ""

def _extract_e_number(name: str) -> Tuple[str, str]:
    """'Sorbic acid (E 200)' → ('E 200','Sorbic acid')"""
    m = re.search(r"\(E\s*([0-9]{3,4})\)", name, flags=re.I)
    e_num = f"E {m.group(1)}" if m else ""
    base = re.sub(r"\(E\s*[0-9]{3,4}\)", "", name).strip()
    return e_num, base

# ▼ 추가: 컬럼 찾기 유틸(정확→부분 일치)
def find_col_fuzzy(df, names: List[str]) -> Optional[str]:
    """
    주어진 후보명들로 DataFrame에서 컬럼을 탐색.
    1) 완전일치 → 2) 부분일치(포함) 순으로 첫 매치 반환.
    """
    low_map = {str(c).strip().lower(): c for c in df.columns}
    # 완전 일치
    for n in names:
        key = str(n).strip().lower()
        if key in low_map:
            return low_map[key]
    # 부분 일치
    for n in names:
        key = str(n).strip().lower()
        for k, orig in low_map.items():
            if key in k:
                return orig
    return None

# ------------- Fuzzy Matching Adapters -------------
ALGO_CHOICES = [
    ("token_set_ratio", "Token Set (best for multi-word)"),
    ("ratio", "Full String Ratio"),
    ("partial_ratio", "Partial Ratio"),
]

def score_pair(query: str, candidate: str, algo_key: str) -> float:
    q = normalize(query)
    c = normalize(candidate)
    if not q or not c:
        return 0.0
    if _USE_RAPIDFUZZ:
        if algo_key == "token_set_ratio":
            return float(fuzz.token_set_ratio(q, c))
        elif algo_key == "partial_ratio":
            return float(fuzz.partial_ratio(q, c))
        else:
            return float(fuzz.ratio(q, c))
    else:
        if algo_key == "token_set_ratio":
            def tok_norm(s: str) -> str:
                toks = set(re.findall(r"\w+", s.lower()))
                return " ".join(sorted(toks))
            qn, cn = tok_norm(q), tok_norm(c)
            return 100.0 * SequenceMatcher(None, qn, cn).ratio()
        elif algo_key == "partial_ratio":
            ql, cl = q.lower(), c.lower()
            best = 0.0
            if len(ql) > len(cl):
                big, small = ql, cl
            else:
                big, small = cl, ql
            window = len(small)
            for i in range(0, len(big) - window + 1):
                seg = big[i:i+window]
                r = SequenceMatcher(None, seg, small).ratio()
                if r > best:
                    best = r
            return 100.0 * best
        else:
            return 100.0 * SequenceMatcher(None, q, c).ratio()

# ---------------- Data Loading & Search -----------------
@dataclass
class DBRow:
    idx: int
    data: Dict[str, Any]

@dataclass
class SearchResult:
    exact_rows: List[DBRow]
    similar_rows: List[Tuple[float, DBRow]]  # (score, row)

class ChemicalDB:
    def __init__(self, kind: str, path: str):
        self.kind = kind  # "KR", "US", "EU"
        self.path = path
        self.df: Optional["pd.DataFrame"] = None
        self.columns_to_display: List[str] = []
        self.cas_col = None
        self.primary_name_col = None
        self.other_names_col = None  # US/EU 동의어
        self.korean_name_col = None  # ✅ 한글명 컬럼
        self.loaded = False

    def load(self):
        if "pandas" in _MISSING_DEPS:
            raise RuntimeError("pandas is required. Please install with: pip install pandas")
        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)

        df = pd.read_excel(self.path)
        # 공통: 엑셀 'Unnamed' 제거
        df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]]
        
        # ▼ 간소화된 분기
        if self.kind == "KR":
            self.cas_col          = find_col_fuzzy(df, ["cas no.", "cas number", "cas"])
            self.primary_name_col = find_col_fuzzy(df, ["영문명", "영문 제품명", "영문", "english name"])
            # 국문명 = 실제 "제품명"(B열) 등
            self.korean_name_col  = find_col_fuzzy(df, ["제품명", "품목명", "국문명", "한글명", "국문", "한글"])

            wanted = [self.cas_col, self.primary_name_col, self.korean_name_col]
            self.columns_to_display = [c for c in wanted if c is not None] or list(df.columns)

        elif self.kind == "US":
            # ① 컬럼 위치 찾기
            self.cas_col          = find_col_fuzzy(df, ["cas reg. no.", "cas reg no", "cas no", "cas"])
            self.primary_name_col = find_col_fuzzy(df, ["substance"])

            # ② 화면 표시 컬럼 = CAS, Substance (두 개만)
            wanted = [self.cas_col, self.primary_name_col]
            self.columns_to_display = [c for c in wanted if c is not None] or list(df.columns)

        elif self.kind == "EU":
            e_col    = find_col_fuzzy(df, ["e_number", "e number", "e-number"])
            name_col = find_col_fuzzy(df, ["additive_name_en", "additive name en", "name_en", "name en"])
            syn_col  = find_col_fuzzy(df, ["synonyms"])
            cas_list_col = find_col_fuzzy(df, ["cas_list", "cas list", "cas reg. no.", "cas no", "cas"])
            food_cat_col = find_col_fuzzy(df, ["food category", "food_category"])
            restr_col    = find_col_fuzzy(df, ["individual restriction(s) / exception(s)", "restrictions", "individual restrictions / exceptions"])
            foot_col     = find_col_fuzzy(df, ["footnotes", "footnote"])

            self.cas_col          = cas_list_col
            self.primary_name_col = name_col
            self.other_names_col  = syn_col

            preferred = [c for c in [e_col, name_col, syn_col, cas_list_col, food_cat_col, restr_col, foot_col] if c]
            rest = [c for c in df.columns if c not in preferred]
            self.columns_to_display = preferred + rest

        else:
            self.columns_to_display = list(df.columns)

        self.df = df.fillna("")
        self.loaded = True

    def _row_candidates(self, row: "pd.Series") -> List[str]:
        cand = []
        kind = self.kind

        # 1) CAS
        if self.cas_col:
            cas_val = row.get(self.cas_col, "")
            if kind == "EU":
                text = str(cas_val)
                parts = re.split(r"[;,/\n]", text)
                cas_tokens = []
                for p in parts:
                    p = (p or "").strip()
                    if not p:
                        continue
                    cas_tokens.extend(CAS_PATTERN.findall(p))
                    if CAS_PATTERN.fullmatch(p):
                        cas_tokens.append(p)
                if not cas_tokens and text.strip():
                    cas_tokens = CAS_PATTERN.findall(text)
                cand.extend(sorted(set(cas_tokens)))
            else:
                cand.append(normalize_cas(cas_val))

        # 2) 대표명(영문)
        if self.primary_name_col:
            cand.append(normalize(row.get(self.primary_name_col, "")))

        # 2-1) (KR) 한글명도 후보에 포함
        if kind == "KR" and self.korean_name_col:
            cand.append(normalize(row.get(self.korean_name_col, "")))

        # 3) 동의어(US/EU)
        if kind in ("US", "EU") and self.other_names_col:
            for token in tokenize_other_names(row.get(self.other_names_col, "")):
                cand.append(token)

        # 4) KR: '이명' 컬럼들
        if kind == "KR" and self.df is not None:
            for col in self.df.columns:
                if "이명" in str(col):
                    syn_tokens = tokenize_other_names(row.get(col, ""))
                    if syn_tokens:
                        cand.extend(syn_tokens)

        # 5) EU: E_Number도 후보
        if kind == "EU":
            e_number_col = next((c for c in self.columns_to_display
                                if str(c).strip().lower() == "e_number"), None)
            if e_number_col:
                en = normalize(row.get(e_number_col, ""))
                if en:
                    cand.append(en)

        # ✅ 반드시 리스트를 반환
        return [x for x in cand if x]

    def search(self, query: str, algo_key: str, threshold: float, max_similar: int = 50) -> SearchResult:
        if not self.loaded:
            self.load()
        q = normalize(query)

        # 🔹 CAS 패턴이 검색어에 포함되면 CAS만 우선 질의로 사용
        m = CAS_PATTERN.search(q)
        if m:
            q = m.group(0)

        q_lower = q.lower()

        exact_rows: List[DBRow] = []
        similar_rows: List[Tuple[float, DBRow]] = []

        for idx, row in self.df.iterrows():
            candidates = self._row_candidates(row)
            is_exact = any(lower(c) == q_lower for c in candidates)
            contains = any(q_lower in lower(c) for c in candidates if c)

            if is_exact:
                exact_rows.append(DBRow(idx=idx, data=row.to_dict()))
                continue

            best_score = 0.0
            for c in candidates:
                s = score_pair(q, c, algo_key)
                if s > best_score:
                    best_score = s
                if contains:
                    best_score = max(best_score, 99.0)

            if best_score >= threshold:
                similar_rows.append((best_score, DBRow(idx=idx, data=row.to_dict())))

        similar_rows.sort(key=lambda x: (-x[0], x[1].idx))
        if len(similar_rows) > max_similar:
            similar_rows = similar_rows[:max_similar]
        return SearchResult(exact_rows=exact_rows, similar_rows=similar_rows)

# ---------- EU Group Page Parser (선택 기능) ----------
def parse_eu_group_page(url: str) -> List[Dict[str, str]]:
    """
    EU 그룹 상세 페이지(예: POL-FAD-IMPORT-3030)에서
    - Group members (첨가물명들)
    - 그룹 단위의 Food category / Restrictions / Footnotes
    를 추출하여 7필드 리스트로 반환.
    """
    headers = {'User-Agent': 'Mozilla/5.0'}
    r = requests.get(url, timeout=20, headers=headers)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")

    # 1) Group members
    members: List[str] = []
    for h in soup.find_all(["h2", "h3", "h4"]):
        if re.search(r"group members", h.get_text(strip=True), flags=re.I):
            ptr = h.find_next_sibling()
            cap = 0
            while ptr and cap < 10:
                txts = []
                if ptr.name == "ul":
                    txts = [li.get_text(" ", strip=True) for li in ptr.find_all("li")]
                elif ptr.name == "table":
                    for tr in ptr.find_all("tr"):
                        tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
                        row = " ".join(tds).strip()
                        if row:
                            txts.append(row)
                if txts:
                    members.extend(txts)
                    break
                ptr = ptr.find_next_sibling()
                cap += 1
            break
    members = [_clean(x) for x in members if _clean(x)]

    # 2) Food category / Restrictions / Footnotes
    def collect_following_lines(title_regex: str) -> List[str]:
        for h in soup.find_all(["h2", "h3", "h4"]):
            if re.search(title_regex, h.get_text(" ", strip=True), flags=re.I):
                ptr = h.find_next_sibling()
                lines = []
                cap = 0
                while ptr and cap < 20:
                    if ptr.name in ("p", "div"):
                        t = ptr.get_text(" ", strip=True)
                        if t: lines.append(t)
                    elif ptr.name == "ul":
                        lines.extend([li.get_text(" ", strip=True) for li in ptr.find_all("li")])
                    elif ptr.name == "table":
                        for tr in ptr.find_all("tr"):
                            tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td","th"])]
                            row = " ".join(tds).strip()
                            if row: lines.append(row)
                    nxt = ptr.find_next_sibling()
                    if nxt and nxt.name in ("h2", "h3", "h4"):
                        break
                    ptr = nxt
                    cap += 1
                return [_clean(x) for x in lines if _clean(x)]
        return []
    categories  = collect_following_lines(r"(food\s*category|characteristics)")
    restrictions = collect_following_lines(r"(restriction|exception)")
    footnotes   = collect_following_lines(r"(footnote|note)")

    cat_join = "\n".join(categories) if categories else ""
    res_join = "\n".join(restrictions) if restrictions else ""
    fn_join  = "\n".join(footnotes) if footnotes else ""

    rows: List[Dict[str, str]] = []
    for m in members:
        e_num, base = _extract_e_number(m)
        rows.append({
            "E_Number": e_num,
            "Additive_Name_EN": base if base else m,
            "Synonyms": "",
            "CAS_List": "",
            "Food category": cat_join,
            "Individual restriction(s) / exception(s)": res_join,
            "Footnotes": fn_join
        })
    if not rows:
        title = soup.find(["h1","h2"])
        title_txt = _clean(title.get_text(" ", strip=True)) if title else ""
        e_num, base = _extract_e_number(title_txt)
        rows = [{
            "E_Number": e_num,
            "Additive_Name_EN": base if base else title_txt or "Unknown",
            "Synonyms": "",
            "CAS_List": "",
            "Food category": cat_join,
            "Individual restriction(s) / exception(s)": res_join,
            "Footnotes": fn_join
        }]
    return rows

# --------------- GUI --------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        # [교체 후]
        self.title("SEMIPRO – Sempio Additives Multi-market Integrated PROgram | Sempio 첨가물 통합검색 프로그램")
        # 스타트는 작게 띄우고, 레이아웃 계산 후 자동으로 맞춤
        self.geometry("1300x540")   # 임시 사이즈(작게)
        # 최소높이 크게 잡지 말아야 축소가 가능
        # self.minsize(...) 는 여기서 안 건드림 (삭제 유지)

    

        if _MISSING_DEPS:
            messagebox.showwarning(
                "Missing dependencies",
                "The following packages are required but not installed:\n- " + "\n- ".join(_MISSING_DEPS) +
                "\n\nInstall them and restart this application."
            )

        # State
        self.kr_db = ChemicalDB("KR", DEFAULT_KR_PATH)
        self.us_db = ChemicalDB("US", DEFAULT_US_PATH)
        self.eu_db = ChemicalDB("EU", DEFAULT_EU_PATH)
        self.algo_key = tk.StringVar(value="token_set_ratio")
        self.threshold = tk.IntVar(value=85)

        # Top
        top = ttk.Frame(self, padding=10)
        top.pack(side=tk.TOP, fill=tk.X)

        ttk.Label(top, text="검색어 (한글명, 영문명, CAS no):", font=("", 11, "bold")).grid(row=0, column=0, sticky="w")
        self.query_var = tk.StringVar()
        q_entry = ttk.Entry(top, textvariable=self.query_var)
        q_entry.grid(row=0, column=1, sticky="we", padx=6)
        q_entry.bind("<Return>", lambda e: self.run_search())

        search_btn = ttk.Button(top, text="검색", command=self.run_search)
        search_btn.grid(row=0, column=2, padx=4)

        clear_btn = ttk.Button(top, text="지우기", command=self.clear_results)
        clear_btn.grid(row=0, column=3, padx=4)

        ttk.Label(top, text="KR 파일:").grid(row=1, column=0, sticky="e", pady=(8,0))
        self.kr_path_var = tk.StringVar(value=DEFAULT_KR_PATH)
        kr_entry = ttk.Entry(top, textvariable=self.kr_path_var)
        kr_entry.grid(row=1, column=1, sticky="we", padx=6, pady=(8,0))
        ttk.Button(top, text="찾기...", command=self.pick_kr).grid(row=1, column=2, padx=4, pady=(8,0))

        ttk.Label(top, text="US 파일:").grid(row=2, column=0, sticky="e", pady=(4,0))
        self.us_path_var = tk.StringVar(value=DEFAULT_US_PATH)
        us_entry = ttk.Entry(top, textvariable=self.us_path_var)
        us_entry.grid(row=2, column=1, sticky="we", padx=6, pady=(4,0))
        ttk.Button(top, text="찾기...", command=self.pick_us).grid(row=2, column=2, padx=4, pady=(4,0))

        ttk.Label(top, text="EU 파일:").grid(row=3, column=0, sticky="e", pady=(4,0))
        self.eu_path_var = tk.StringVar(value=DEFAULT_EU_PATH)
        eu_entry = ttk.Entry(top, textvariable=self.eu_path_var)
        eu_entry.grid(row=3, column=1, sticky="we", padx=6, pady=(4,0))
        ttk.Button(top, text="찾기...", command=self.pick_eu).grid(row=3, column=2, padx=4, pady=(4,0))

        cfg = ttk.Frame(top)
        cfg.grid(row=0, column=4, rowspan=4, padx=(20, 0), sticky="nsw")
        ttk.Label(cfg, text="유사도 설정", font=("", 10, "bold")).grid(row=0, column=0, columnspan=2, sticky="w")

        ttk.Label(cfg, text="알고리즘:").grid(row=1, column=0, sticky="e")
        self.algo_combo = ttk.Combobox(cfg, values=[f"{k} – {label}" for k, label in ALGO_CHOICES], state="readonly")
        self.algo_combo.current(0)
        self.algo_combo.grid(row=1, column=1, sticky="w")
        self.algo_combo.bind("<<ComboboxSelected>>", self._on_algo_changed)

        ttk.Label(cfg, text="임계값:").grid(row=2, column=0, sticky="e")
        thr = ttk.Scale(cfg, from_=50, to=100, orient="horizontal", variable=self.threshold, command=lambda e: self._update_thr_label())
        thr.grid(row=2, column=1, sticky="we")
        self.thr_label = ttk.Label(cfg, text=f"{self.threshold.get()}")
        self.thr_label.grid(row=2, column=2, sticky="w")

        if not _USE_RAPIDFUZZ:
            ttk.Label(cfg, foreground="orange", text="참고: rapidfuzz 미설치 → difflib 대체 사용").grid(row=3, column=0, columnspan=3, sticky="w", pady=(6,0))

        # Main Paned Window
        paned = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        # 세로로 남는 공간을 먹지 않도록 expand=False
        paned.pack(fill=tk.BOTH, expand=True, padx=10, pady=6)


        self.kr_frame = self._make_db_panel(paned, "대한민국 (KR DB)", kind="KR")
        self.us_frame = self._make_db_panel(paned, "미국 (US DB)", kind="US")
        self.eu_frame = self._make_db_panel(paned, "유럽 (EU DB)", kind="EU")
        paned.add(self.kr_frame, weight=1)
        paned.add(self.us_frame, weight=1)
        paned.add(self.eu_frame, weight=1)

        # Bottom status
        self.status_bar = tk.Text(self, height=1, relief=tk.SUNKEN, padx=8, pady=4)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        self.status_bar.configure(state="disabled")

        top.grid_columnconfigure(1, weight=1)
        # 처음 뜰 때 창 높이를 '유사 검색 결과' 하단까지만 맞춤
        self.after(50, self._fit_window_to_sim_trees)

    # ---------- Small helpers ----------
    def _on_algo_changed(self, event=None):
        val = self.algo_combo.get().split(" – ")[0].strip()
        if val:
            self.algo_key.set(val)

    def _update_thr_label(self):
        self.thr_label.configure(text=f"{int(self.threshold.get())}")

    def _parse_tsv(self, tsv: str) -> tuple[list[str], list[list[str]]]:
        lines = [ln.strip("\r") for ln in (tsv or "").split("\n") if ln.strip()]
        if not lines:
            return [], []
        headers = [h.strip() for h in lines[0].split("\t")]
        rows = []
        for ln in lines[1:]:
            cells = [c.strip() for c in ln.split("\t")]
            if len(cells) < len(headers):
                cells += [""] * (len(headers) - len(cells))
            elif len(cells) > len(headers):
                cells = cells[:len(headers)]
            rows.append(cells)
        return headers, rows
    
    def _render_inline_table(self, parent: tk.Widget, headers: list[str], rows: list[list[str]]):
        # 홀더 프레임 비우기(재실행 대비)
        for w in parent.winfo_children():
            w.destroy()

        tbl = ttk.Treeview(parent, columns=headers, show="headings")
        for h in headers:
            tbl.heading(h, text=h)
            tbl.column(h, width=180, anchor="w", stretch=True)

        vsb = ttk.Scrollbar(parent, orient="vertical", command=tbl.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=tbl.xview)
        tbl.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tbl.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=1)

        for r in rows:
            tbl.insert("", "end", values=r)

    def _estimate_ai_latency(self, text: str) -> float:
        """
        사용기준 원문 길이 기반으로 Gemini 응답 ETA(초) 대충 추정.
        - 최소 2.0s ~ 최대 12.0s 사이
        - 길면 더 길게 잡음(대충 체감치)
        """
        if not text:
            return 2.0
        chars = len(text)
        words = max(1, len(text.split()))
        # 기본 2.5초 + (문자/2200) * 6초 + (단어/700) * 3초 → 캡핑
        sec = 2.5 + (chars / 2200.0) * 6.0 + (words / 700.0) * 3.0
        return max(2.0, min(12.0, sec))


    def _ai_usage_table_inline(self, usage_text: str, holder: ttk.Frame):
        # 홀더 비우기(재실행 대비)
        for w in holder.winfo_children():
            w.destroy()

        if not GEMINI_CONFIGURED:
            ttk.Label(holder, text="Gemini API 키 설정이 안 돼서 표 변환 비활성화됨.", foreground="gray").grid(sticky="w")
            return
        if not usage_text or not usage_text.strip():
            ttk.Label(holder, text="사용기준 원문이 없어서 표 변환 불가.", foreground="gray").grid(sticky="w")
            return

        # --- 게이지 UI (determinate + ETA 기반) ---
        g_frame = ttk.Frame(holder)
        g_frame.grid(row=0, column=0, sticky="we")
        g_frame.grid_columnconfigure(0, weight=1)
        pb = ttk.Progressbar(g_frame, mode="determinate", maximum=100)
        pb.grid(row=0, column=0, sticky="we", padx=2, pady=4)
        msg = ttk.Label(g_frame, text="AI가 사용기준을 표로 정리 중…")
        msg.grid(row=1, column=0, sticky="w")

                # ETA 세팅
        import time, threading, re
        start_ts = time.time()
        eta_sec = self._estimate_ai_latency(usage_text)

        # 상태 플래그 / 예측 종료시각
        holder._ai_loading = True
        holder._ai_done = False
        holder._pred_done_ts = start_ts + eta_sec
        holder._last_tick_ts = start_ts  # 최소 상승속도 계산용

        # holder 파괴되면 루프 중단
        try:
            holder.bind("<Destroy>", lambda e: setattr(holder, "_ai_loading", False))
        except Exception:
            pass

        # 안전 가드
        def _alive(w):
            try:
                return bool(w.winfo_exists())
            except Exception:
                return False

        # 진행률 파라미터
        MIN_RATE_PPS = 5.0   # 초당 최소 % 상승 (멈춘 느낌 방지)
        TICK_MS = 80
        LATE_PUSH_SEC = 1.2
        NEARLY_DONE = 98.0

        def tick():
            # 이미 종료되었거나 위젯이 없으면 중단
            if not getattr(holder, "_ai_loading", False) or not _alive(pb):
                return

            now = time.time()
            cur = float(pb["value"])
            pred_done = getattr(holder, "_pred_done_ts", start_ts + eta_sec)

            # ETA 오버런 시, 예측 종료시각 당겨서 계속 전진
            if now >= pred_done and not getattr(holder, "_ai_done", False):
                holder._pred_done_ts = now + LATE_PUSH_SEC
                pred_done = holder._pred_done_ts

            # ETA 기반 목표치 (완만한 이징)
            denom = max(0.8, pred_done - start_ts)
            ratio = (now - start_ts) / denom
            eased = pow(max(0.0, min(1.0, ratio)), 0.85) * 100.0
            eta_target = max(1.0, min(NEARLY_DONE, eased))

            # 최소 상승 속도 보장
            dt = max(0.0, now - getattr(holder, "_last_tick_ts", now))
            min_target = min(NEARLY_DONE, cur + (MIN_RATE_PPS * dt))
            target = max(eta_target, min_target)

            # 95% 이상 + ETA의 80% 경과 → 가속 밀어주기
            if (cur >= 95.0 and not getattr(holder, "_ai_done", False) and
                (now - start_ts) >= 0.8 * eta_sec):
                target = max(target, min(NEARLY_DONE, cur + 1.0))

            # 적용
            if target > cur and _alive(pb):
                pb["value"] = target

            holder._last_tick_ts = now
            if getattr(holder, "_ai_loading", False):
                holder.after(TICK_MS, tick)

        pb["value"] = 1.0
        holder.after(TICK_MS, tick)




        # --- 백그라운드: TSV 생성 ---
        def worker():
            try:
                prompt = (
                    "다음 '사용기준' 한국어 원문을 표로 구조화해줘.\n"
                    "- 컬럼은 가능하면 다음 순서로: 식품유형\t허용기준(수치)\t근거/조건\t비고\n"
                    "- 수치는 mg/kg, mg/L, %, 또는 GMP/quantum satis 등 명확표기.\n"
                    "- EU 같은 경우 Part E 코드/범주명도 '식품유형'에 포함.\n"
                    "- 사용 불가/제한 문구도 행으로 표시.\n"
                    "- 반드시 **TSV(탭 구분)** 만 출력. 마크다운/설명문/코드블록 금지.\n\n"
                    f"원문:\n{usage_text[:6000]}"
                )
                # 실제 호출 (gRPC/absl 잡로그 무음화)
                with _silence_stderr_c():
                    resp = GEMINI_MODEL.generate_content(prompt).text or ""

                tsv = resp.strip()
                if tsv.startswith("```"):
                    tsv = re.sub(r"^```[a-zA-Z]*", "", tsv).strip()
                    tsv = tsv[:-3] if tsv.endswith("```") else tsv

                headers, rows = self._parse_tsv(tsv)
                if not headers or not rows:
                    raise ValueError("TSV 파싱 실패")

                def on_ok():
                    # 완료 신호 + tick 루프 중단
                    holder._ai_done = True
                    holder._ai_loading = False
                    holder._pred_done_ts = time.time()

                    def finalize_to_100():
                        # 위젯이 이미 없어졌으면 중단
                        if not _alive(pb):
                            return
                        try:
                            cur = float(pb["value"])
                        except Exception:
                            return
                        if cur < 100.0:
                            # 부드럽게 가속
                            inc = max(1.5, (100.0 - cur) * 0.5)
                            try:
                                pb["value"] = min(100.0, cur + inc)
                            except Exception:
                                return
                            if _alive(holder):
                                holder.after(30, finalize_to_100)
                            return

                        # 100% 찍고 잠깐 보여준 뒤 스왑
                        def swap():
                            if not _alive(holder):
                                return
                            # 진행 UI 안전하게 정리
                            try:
                                holder._ai_loading = False
                            except Exception:
                                pass
                            for w in list(holder.winfo_children()):
                                try:
                                    w.destroy()
                                except Exception:
                                    pass
                            # 표 렌더
                            self._render_inline_table(holder, headers, rows)

                        if _alive(holder):
                            holder.after(120, swap)

                    finalize_to_100()

                self.after(0, on_ok)

            except Exception as e:
                def on_fail():
                    holder._ai_loading = False
                    for w in holder.winfo_children(): w.destroy()
                    ttk.Label(holder, text=f"AI 표 변환 실패: {e}", foreground="orange").grid(sticky="w")
                self.after(0, on_fail)

        th = threading.Thread(target=worker, daemon=True)
        th.start()

    def _extract_cas_from_row_dict(self, db: "ChemicalDB", row_dict: dict) -> str:
        """
        KR exact row에서 CAS 하나만 안전하게 추출해서 리턴함.
        EU처럼 한 셀에 여러 개 섞여 있어도 정규식으로 첫 매치만 뽑아줌.
        """
        val = ""
        try:
            if db.cas_col:
                val = str(row_dict.get(db.cas_col, "") or "")
        except Exception:
            val = ""
        m = CAS_PATTERN.search(val)
        return m.group(0) if m else ""
    
    def _ai_usage_table_popup(self, usage_text: str, parent: tk.Toplevel):
        if not GEMINI_CONFIGURED:
            messagebox.showerror("API 오류", "Gemini API 키 설정 안됨. GOOGLE_API_KEY 넣고 다시 ㄱㄱ")
            return
        if not usage_text or not usage_text.strip():
            messagebox.showinfo("정보 없음", "사용기준 원문이 없음.")
            return

        wait = tk.Toplevel(parent)
        wait.title("AI 표 생성 중…")
        ttk.Label(wait, text="Gemini가 사용기준을 표로 정리 중...").pack(padx=16, pady=16)
        wait.geometry("360x120")
        wait.transient(parent)
        wait.grab_set()
        wait.update_idletasks()

        def worker():
            try:
                prompt = (
                    "다음 '사용기준' 한국어 원문을 표로 구조화해줘.\n"
                    "- 컬럼은 가능하면 다음 순서로: 식품유형\t허용기준(수치)\t근거/조건\t비고\n"
                    "- 수치는 mg/kg, mg/L, %, 또는 GMP/quantum satis 등 명확표기.\n"
                    "- EU 같은 경우 Part E 코드/범주명도 '식품유형'에 포함.\n"
                    "- 사용 불가/제한 문구도 행으로 표시.\n"
                    "- 반드시 **TSV(탭 구분)** 만 출력. 마크다운/설명문/코드블록 금지.\n\n"
                    f"원문:\n{usage_text[:6000]}"
                )
                resp = GEMINI_MODEL.generate_content(prompt).text or ""
                # 혹시 코드블록 들어오면 걍 벗겨냄
                resp = resp.strip()
                if resp.startswith("```"):
                    resp = re.sub(r"^```[a-zA-Z]*", "", resp).strip()
                    resp = resp[:-3] if resp.endswith("```") else resp

                headers, rows = self._parse_tsv(resp)
                if not headers or not rows:
                    raise ValueError("TSV 파싱 실패")

                self.after(0, lambda: (wait.destroy(),
                                    self._show_table_modal(parent, "사용기준 – AI 표 요약", headers, rows)))
            except Exception as e:
                self.after(0, lambda: (wait.destroy(),
                                    messagebox.showerror("표 변환 실패", f"AI 표 변환 실패: {e}")))
        th = threading.Thread(target=worker, daemon=True)
        th.start()


    def pick_kr(self):
        path = filedialog.askopenfilename(title="KR 엑셀 파일 선택", filetypes=[("Excel", "*.xlsx *.xls")])
        if path:
            self.kr_path_var.set(path)
            self.kr_db.path = path
            self.kr_db.loaded = False

    def pick_us(self):
        path = filedialog.askopenfilename(title="US 엑셀 파일 선택", filetypes=[("Excel", "*.xlsx *.xls")])
        if path:
            self.us_path_var.set(path)
            self.us_db.path = path
            self.us_db.loaded = False

    def pick_eu(self):
        path = filedialog.askopenfilename(title="EU 엑셀 파일 선택", filetypes=[("Excel", "*.xlsx *.xls")])
        if path:
            self.eu_path_var.set(path)
            self.eu_db.path = path
            self.eu_db.loaded = False

    # ---------- Collapsible text ----------
    def _make_collapsible_text(self, parent, full_text: str, max_lines: int = 2):
        wrapper = ttk.Frame(parent)
        wrapper.grid_columnconfigure(0, weight=1)

        txt = tk.Text(wrapper, wrap="word", relief="flat",
                      height=max_lines,
                      background=parent.winfo_toplevel().cget('bg'),
                      state="normal")
        txt.insert("1.0", full_text or "")
        txt.configure(state="disabled")
        txt.grid(row=0, column=0, sticky="we")

        def _block_scroll(event):
            return "break"

        btn = ttk.Button(wrapper, text="펼치기")
        btn.grid(row=0, column=1, sticky="ne", padx=(6, 0))
        state = {"expanded": False}

        def toggle():
            if state["expanded"]:
                txt.configure(state="normal")
                txt.configure(height=max_lines)
                txt.configure(state="disabled")
                btn.configure(text="펼치기")
                txt.bind("<MouseWheel>", _block_scroll)
                txt.bind("<Button-4>", _block_scroll)
                txt.bind("<Button-5>", _block_scroll)
                state["expanded"] = False
            else:
                txt.configure(state="normal")
                try:
                    txt.update_idletasks()
                    total_display_lines = int(txt.count("1.0", "end-1c", "displaylines")[0])
                except Exception:
                    txt.update_idletasks()
                    width_px = max(1, txt.winfo_width())
                    chars_per_line = max(10, width_px // 7)
                    content = txt.get("1.0", "end-1c")
                    logical_lines = content.splitlines() or [""]
                    est = 0
                    for ln in logical_lines:
                        est += max(1, (len(ln) // chars_per_line) + (1 if len(ln) % chars_per_line else 0))
                    total_display_lines = est

                txt.configure(height=max(total_display_lines, max_lines + 1))
                txt.configure(state="disabled")
                btn.configure(text="숨기기")
                txt.unbind("<MouseWheel>")
                txt.unbind("<Button-4>")
                txt.unbind("<Button-5>")
                state["expanded"] = True

        btn.configure(command=toggle)
        txt.bind("<MouseWheel>", _block_scroll)
        txt.bind("<Button-4>", _block_scroll)
        txt.bind("<Button-5>", _block_scroll)
        return wrapper

    # ---------- Panels & Tree ----------
    def _make_db_panel(self, parent, title: str, kind: str) -> ttk.Labelframe:
        frame = ttk.Labelframe(parent, text=title, padding=8)
        ttk.Label(frame, text="완벽히 일치하는 것", font=("", 10, "bold")).pack(anchor="w")
        exact_tree = self._build_tree(frame, height=4)
        exact_tree._db_kind = kind
        exact_tree.pack(fill=tk.BOTH, expand=False, pady=(0,8))

        ttk.Label(frame, text="유사 검색 결과", font=("", 10, "bold")).pack(anchor="w")
        # 70%로 줄인 행수(기존 10행 → 7행)
        sim_tree = self._build_tree(frame, show_score=False, height=7)
        sim_tree._db_kind = kind
        # 더 이상 남는 세로 공간을 다 먹지 않도록 expand=False
        sim_tree.pack(fill=tk.BOTH, expand=True)
        self._attach_mousewheel(exact_tree)
        self._attach_mousewheel(sim_tree)


        frame.exact_tree = exact_tree
        frame.sim_tree = sim_tree
        return frame

    def _fit_window_to_sim_trees(self, extra_pad: int = 8, max_ratio: float = 0.92):
        """
        메인 창 높이를 '세 패널의 유사검색결과(sim_tree) 중 가장 아래쪽 y' + 상태바 높이까지만 맞춘다.
        extra_pad: 소량의 여유(px), max_ratio: 화면 높이 대비 최대 비율
        """
        try:
            # 레이아웃 계산을 안정화
            self.update_idletasks()

            # sim_tree들 중 가장 아래 y 좌표(루트 기준) 계산
            bottom = 0
            for f in (self.kr_frame, self.us_frame, self.eu_frame):
                st = getattr(f, "sim_tree", None)
                if not st:
                    continue
                y_rel = st.winfo_rooty() - self.winfo_rooty()
                h = st.winfo_height() if st.winfo_height() > 1 else st.winfo_reqheight()
                b = y_rel + h
                if b > bottom:
                    bottom = b

            # 상태바 높이(요구치 기반) + PanedWindow 하단 패딩(pady=6)을 더함
            sb_h = (self.status_bar.winfo_reqheight() or 18)
            pady_bottom = 6
            target_h = int(bottom + sb_h + pady_bottom + extra_pad)

            # 현재 너비 유지, 화면 상한 캡
            cur_w = self.winfo_width() if self.winfo_width() > 1 else 1500
            screen_h = self.winfo_screenheight()
            cap_h = int(screen_h * max_ratio)
            final_h = min(target_h, cap_h)

            # 현재 위치 유지
            pos_x = self.winfo_x() if self.winfo_x() > 0 else 50
            pos_y = self.winfo_y() if self.winfo_y() > 0 else 50

            self.geometry(f"{cur_w}x{final_h}+{pos_x}+{pos_y}")
            
            # 드문 케이스 보정(한 번 더)
            self.update_idletasks()

            # ← 추가: 내용 높이를 최소치로 고정
            self.minsize(cur_w, final_h)
        except Exception:
            pass



    def _build_tree(self, parent, show_score: bool = False, height: int = 10) -> ttk.Treeview:
        tree = ttk.Treeview(parent, columns=(ROW_ID_COL,), show="headings", height=height)
        vsb = ttk.Scrollbar(parent, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(parent, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)

        tree.bind("<Double-1>", lambda e, t=tree: self._on_tree_double_click(e, t))
        menu = tk.Menu(tree, tearoff=0)
        menu.add_command(label="자세히 보기 / 링크 열기", command=lambda t=tree: self.open_detail_popup(t))
        tree.bind("<Button-3>", lambda e, m=menu: (m.tk_popup(e.x_root, e.y_root)))

        tree.heading(ROW_ID_COL, text=ROW_ID_COL)
        tree.column(ROW_ID_COL, width=0, stretch=False)  # 숨김

        tree._show_score = show_score
        return tree
    
    def _attach_mousewheel(self, tree: ttk.Treeview):
        # Win / macOS (e.delta 사용)
        def _on_mousewheel(event):
            if event.delta:
                tree.yview_scroll(-int(event.delta/120), "units")
                return "break"
        # Linux (Button-4/5)
        def _on_btn4(event):
            tree.yview_scroll(-1, "units"); return "break"
        def _on_btn5(event):
            tree.yview_scroll( 1, "units"); return "break"

        tree.bind("<MouseWheel>", _on_mousewheel)
        tree.bind("<Button-4>", _on_btn4)
        tree.bind("<Button-5>", _on_btn5)


    def _on_tree_double_click(self, event, tree: ttk.Treeview):
        """
        - 구분선(separator)에서 더블클릭: 왼쪽 컬럼을 '헤더 텍스트 길이' 기준으로 자동폭
        - 헤더(heading) 영역 더블클릭: 아무것도 안 함(기존 팝업 열리지 않도록 차단)
        - 그 외(바디 셀) 더블클릭: 기존 상세 팝업 열기
        """
        region = tree.identify_region(event.x, event.y)
        if region == "separator":
            # 구분선 더블클릭 → 왼쪽 컬럼 자동폭
            self._auto_width_left_header_by_separator(event, tree)
            return "break"
        elif region == "heading":
            # 헤더 텍스트 더블클릭은 무시 (팝업 안 뜨게)
            return "break"
        else:
            # 셀 더블클릭은 기존 동작 유지
            self.open_detail_popup(tree)
            return "break"


    def _auto_width_left_header_by_separator(self, event, tree: ttk.Treeview,
                                            padding: int = 28, min_w: int = 90, max_w: int = 520):
        """
        헤더 구분선에서 더블클릭한 경우:
        - 구분선의 '왼쪽' 컬럼을 헤더 텍스트 길이에 맞춰 자동폭으로 조정
        - 마지막 컬럼이 남은 공간을 모두 차지하도록 재밸런싱
        """
        from tkinter import font as tkfont

        # 구분선에서 왼쪽 컬럼 인덱스('#1'.. 형태)를 얻어 컬럼명으로 변환
        col_index_str = tree.identify_column(event.x)  # '#1', '#2', ...
        if not col_index_str or not col_index_str.startswith("#"):
            return
        idx = int(col_index_str[1:]) - 1
        cols_all = list(tree["columns"])
        if idx < 0 or idx >= len(cols_all):
            return

        colname = cols_all[idx]
        # 숨김 인덱스 컬럼은 스킵
        if colname == ROW_ID_COL:
            return

        # 헤더 텍스트 측정
        try:
            tv_font = tkfont.nametofont(tree.cget("font"))
        except Exception:
            tv_font = tkfont.Font()

        header_text = tree.heading(colname, "text") or str(colname)
        target_px = tv_font.measure(header_text) + padding
        new_w = max(min_w, min(max_w, target_px))

        # 왼쪽(대상) 컬럼 폭만 갱신
        tree.column(colname, width=new_w, stretch=False, minwidth=min_w)

        # 마지막 컬럼이 남은 공간을 전부 차지하도록 재밸런싱
        self._rebalance_last_column(tree, min_w=min_w)


    def _rebalance_last_column(self, tree: ttk.Treeview, min_w: int = 90):
        """
        - 마지막 컬럼은 stretch=True로 두고,
        - 나머지는 stretch=False 고정
        - 현재 Treeview 폭 기준으로 '마지막 컬럼' 폭을 남은 영역만큼 재설정
        """
        tree.update_idletasks()
        total_w = tree.winfo_width()
        cols_all = list(tree["columns"])
        # 숨김 인덱스 제외한 실제 표시 컬럼
        cols = [c for c in cols_all if c != ROW_ID_COL]
        if not cols:
            return

        # 마지막 컬럼
        last_col = cols[-1]

        # 앞의 컬럼들의 현재 폭 합산(고정)
        used = 0
        for c in cols[:-1]:
            cur_w = int(tree.column(c, "width"))
            tree.column(c, stretch=False)  # 고정
            used += cur_w

        # 남은 영역을 마지막 컬럼이 먹도록
        remaining = max(min_w, (total_w - used) if total_w > 0 else int(tree.column(last_col, "width")))
        tree.column(last_col, width=remaining, stretch=True, minwidth=min_w)


    def _autosize_tree_columns(self, tree: ttk.Treeview, padding: int = 28,
                           min_w: int = 90, max_w: int = 520,
                           sample_rows: int = 80):
        """
        - 헤더(항목명) 텍스트를 최우선으로 폭을 잡고,
        - 셀 일부를 샘플링해 필요 시 폭을 키우며,
        - 마지막 열이 남은 가용 폭을 모두 차지하도록(stretch=True),
        나머지 열은 고정(stretch=False)한다.
        """
        from tkinter import font as tkfont

        try:
            tv_font = tkfont.nametofont(tree.cget("font"))
        except Exception:
            tv_font = tkfont.Font()

        # 렌더링 전이면 width=1일 수 있으니 한 번 레이아웃 강제
        tree.update_idletasks()
        total_w = tree.winfo_width()
        if total_w <= 1:
            # 그래도 못 받으면 부모 폭을 참조(대략치)
            parent = tree.nametowidget(tree.winfo_parent())
            parent.update_idletasks()
            total_w = max(total_w, parent.winfo_width())

        # 표시 컬럼들(숨김 인덱스 제외)
        cols_all = list(tree["columns"])
        cols = [c for c in cols_all if c != ROW_ID_COL]
        if not cols:
            return

        # 각 컬럼의 "요청 폭" 계산(헤더 우선 + 일부 셀 샘플)
        desired = {}
        items = tree.get_children("")
        if sample_rows and len(items) > sample_rows:
            items = items[:sample_rows]

        for col in cols:
            header_text = tree.heading(col, "text") or str(col)
            max_px = tv_font.measure(header_text)

            for iid in items:
                cell_text = tree.set(iid, col)
                if cell_text:
                    px = tv_font.measure(str(cell_text))
                    if px > max_px:
                        max_px = px

            width = max(min_w, min(max_w, max_px + padding))
            desired[col] = width

        # 마지막 열이 남은 공간을 채우도록 설정
        last_col = cols[-1]
        # 우선 다른 열들 고정 폭 적용
        used_except_last = 0
        for col in cols[:-1]:
            w = desired[col]
            tree.column(col, width=w, stretch=False, minwidth=min_w)
            used_except_last += w

        # 남은 폭 계산(음수면 최소폭으로)
        remaining = (total_w - used_except_last) if total_w > 0 else desired[last_col]
        last_width = max(min_w, remaining)
        # 너무 크게 벌어지면 max_w로 클램프하지 않고, stretch로 남는 영역을 먹게 둠
        tree.column(last_col, width=last_width, stretch=True, minwidth=min_w)

    def _get_from_row(self, row_dict: dict, candidates: List[str]) -> str:
        """
        행 딕셔너리에서 후보 키들(정확→부분 일치)로 값을 안전하게 꺼낸다.
        공백/대소문자 차이는 무시.
        """
        if not row_dict:
            return ""
        low = {str(k).strip().lower(): k for k in row_dict.keys()}

        # 1) 정확 일치
        for cand in candidates:
            if not cand:
                continue
            key = str(cand).strip().lower()
            if key in low:
                val = row_dict.get(low[key], "")
                return "" if val is None else str(val)

        # 2) 부분 포함 매치
        for cand in candidates:
            if not cand:
                continue
            key = str(cand).strip().lower()
            for k_low, orig in low.items():
                if key and key in k_low:
                    val = row_dict.get(orig, "")
                    return "" if val is None else str(val)

        return ""

    def _render_kr_detail(self, frame: ttk.Frame, full_row: dict,
                      db: "ChemicalDB",
                      summary_text: "tk.Text", chat_history: "tk.Text",
                      chat_entry: "ttk.Entry", chat_btn: "ttk.Button") -> tuple[int, str]:
        """
        KR 상세 팝업을 섹션/필드 순서대로 렌더링.
        반환: (다음 row 인덱스, '사용기준' 원문 텍스트)
        """
        frame.grid_columnconfigure(0, weight=0)
        frame.grid_columnconfigure(1, weight=1)

        current_row = 0

        def add_section(title: str):
            nonlocal current_row
            if current_row > 0:
                sep = ttk.Separator(frame, orient="horizontal")
                sep.grid(row=current_row, column=0, columnspan=2, sticky="ew", pady=(12, 8))
                current_row += 1
            lbl = ttk.Label(frame, text=title, font=("", 11, "bold"))
            lbl.grid(row=current_row, column=0, columnspan=2, sticky="w", pady=(0, 6))
            current_row += 1

        def add_field(label: str, value: str, fold: bool = False):
            nonlocal current_row
            ttk.Label(frame, text=label, font=("", 10, "bold"))\
                .grid(row=current_row, column=0, sticky="ne", padx=(0, 8), pady=3)

            val_frame = ttk.Frame(frame)
            val_frame.grid(row=current_row, column=1, sticky="we", pady=3)

            text = value or ""
            if fold:
                self._make_collapsible_text(val_frame, text, max_lines=3)\
                    .pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            else:
                t = tk.Text(val_frame,
                            height=min(6, max(1, (len(text) // 80) + 1)),
                            wrap="word", relief="flat",
                            background=frame.winfo_toplevel().cget('bg'))
                t.insert("1.0", text)
                t.configure(state="disabled")
                t.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

            urls = extract_urls(text)
            if urls:
                link_frame = ttk.Frame(val_frame, padding=(6, 0))
                link_frame.pack(side=tk.LEFT)
                for u in urls:
                    ttk.Button(link_frame, text="AI 요약",
                            command=lambda url=u, st=summary_text, ch=chat_history, ce=chat_entry, cb=chat_btn:
                                self._start_summary_process(url, st, ch, ce, cb)).pack(anchor="w", pady=2)
            current_row += 1

        # ─ 값 추출 ─
        eng = self._get_from_row(full_row, [db.primary_name_col or "", "영문명", "영문 제품명", "영문", "english name", "substance", "name"])
        cas = self._get_from_row(full_row, [db.cas_col or "", "cas no.", "cas number", "cas reg. no.", "cas"])
        primary_use = self._get_from_row(full_row, ["주용도", "주 용도", "용도", "사용용도"])
        usage = self._get_from_row(full_row, ["사용기준", "사용 기준", "사용기준(국내기준)", "사용기준(국내기준)_업데이트"])
        kor = self._get_from_row(full_row, [db.korean_name_col or "", "제품명", "국문명", "한글명", "품목명"])
        synonyms = self._get_from_row(full_row, ["이명", "동의어", "별칭", "synonym", "synonyms", "other names"])
        formula = self._get_from_row(full_row, ["분자식", "화학식", "molecular formula", "formula"])
        mw = self._get_from_row(full_row, ["분자량", "molecular weight", "mw"])
        ins = self._get_from_row(full_row, ["ins.no", "ins no", "ins", "ins.no.", "ins no.", "INS.No", "INS No"])

        # ─ 섹션/필드 렌더 ─
        add_section("주요정보")
        add_field("영문명", eng)
        add_field("CAS No.", cas)

        add_section("사용정보")
        add_field("주용도", primary_use)

        # --- [추가] 사용기준 – AI 표 요약 (원문 '위'에 표시) ---
        ttk.Label(frame, text="사용기준 – AI 표 요약", font=("", 10, "bold"))\
            .grid(row=current_row, column=0, columnspan=2, sticky="w", pady=(2, 2))
        current_row += 1
        ai_table_holder = ttk.Frame(frame)
        ai_table_holder.grid(row=current_row, column=0, columnspan=2, sticky="nsew", pady=(0, 6))
        frame.grid_rowconfigure(current_row, weight=1)
        current_row += 1
        # 표 생성 시작 (비동기 + 게이지)
        self._ai_usage_table_inline(usage, ai_table_holder)

        add_field("사용기준", usage, fold=True)   # ← Gemini 컨텍스트(원문 반환)
        add_section("추가정보")
        add_field("국문명", kor)
        add_field("이명", synonyms)
        add_field("분자식", formula)
        add_field("분자량", mw)
        add_field("INS.No", ins)

        return current_row, (usage or "")


    # ▼ 헤더 라벨 계산(유사도 점수/국문명)
    def _header_label(self, db: ChemicalDB, col: str, is_score_col: bool) -> str:
        if is_score_col:
            return "유사도 점수"
        if db.kind == "KR":
            if getattr(db, "korean_name_col", None) and col == db.korean_name_col:
                return "국문명"
            if str(col).strip() == "제품명":
                return "국문명"
        return str(col)

    # ---------- Core Search ----------
    def _ensure_load(self) -> bool:
        try:
            new_kr_path = self.kr_path_var.get().strip() or DEFAULT_KR_PATH
            new_us_path = self.us_path_var.get().strip() or DEFAULT_US_PATH
            new_eu_path = self.eu_path_var.get().strip() or DEFAULT_EU_PATH

            if self.kr_db.path != new_kr_path:
                self.kr_db.path = new_kr_path
                self.kr_db.loaded = False
            if self.us_db.path != new_us_path:
                self.us_db.path = new_us_path
                self.us_db.loaded = False
            if self.eu_db.path != new_eu_path:
                self.eu_db.path = new_eu_path
                self.eu_db.loaded = False

            if not self.kr_db.loaded:
                self.kr_db.load()
            if not self.us_db.loaded:
                self.us_db.load()
            if not self.eu_db.loaded:
                self.eu_db.load()
            return True
        except FileNotFoundError as e:
            messagebox.showerror("파일을 찾을 수 없음", f"파일이 존재하지 않습니다:\n{e}")
        except Exception as e:
            messagebox.showerror("로딩 오류", f"데이터베이스 로딩 중 오류 발생:\n{e}")
        return False

    def run_search(self):
        if not self._ensure_load():
            return

        query = self.query_var.get().strip()
        if not query:
            messagebox.showinfo("입력 필요", "검색어를 입력하세요 (한글명, 영문명 또는 CAS no).")
            return

        algo = (self.algo_combo.get().split(" – ")[0].strip() if self.algo_combo.get() else "token_set_ratio") or self.algo_key.get()
        thr = float(self.threshold.get())

        # 작업 중 커서 표시
        self.config(cursor="watch")
        self.update_idletasks()

        def worker():
            try:
                # ✅ 무거운 검색 로직은 백그라운드에서 수행
                kr_res = self.kr_db.search(query, algo_key=algo, threshold=thr)
                us_res = self.us_db.search(query, algo_key=algo, threshold=thr)
                eu_res = self.eu_db.search(query, algo_key=algo, threshold=thr)
                eu_exact_compact = self._eu_dedupe_exact(eu_res.exact_rows)

                # --- [추가] KR 정확일치가 있으면 CAS 하나 뽑아서 US/EU도 exact 매칭 보정 ---
                try:
                    if kr_res.exact_rows:
                        cas = self._extract_cas_from_row_dict(self.kr_db, kr_res.exact_rows[0].data)
                        if cas:
                            # US: exact 없다면 CAS로 한 번 더 질의해서 exact 채움
                            if not us_res.exact_rows:
                                us_cas_res = self.us_db.search(cas, algo_key="ratio", threshold=float(self.threshold.get()))
                                if us_cas_res.exact_rows:
                                    us_res = SearchResult(exact_rows=us_cas_res.exact_rows, similar_rows=us_res.similar_rows)

                            # EU: exact 없다면 CAS로 재질의 + 그룹 중복 정리
                            if not eu_res.exact_rows:
                                eu_cas_res = self.eu_db.search(cas, algo_key="ratio", threshold=float(self.threshold.get()))
                                if eu_cas_res.exact_rows:
                                    eu_res = SearchResult(
                                        exact_rows=self._eu_dedupe_exact(eu_cas_res.exact_rows),
                                        similar_rows=eu_res.similar_rows
                                    )
                except Exception:
                    # 브릿지 실패해도 메인 검색은 정상 진행
                    pass

                # (중요) 위에서 eu_res 바뀌었을 수 있으니까, compact 다시 계산
                eu_exact_compact = self._eu_dedupe_exact(eu_res.exact_rows)


                # ✅ UI 변경은 반드시 메인 스레드에서
                def apply_results():
                    # KR
                    self._populate_tree(self.kr_frame.exact_tree, self.kr_db, kr_res.exact_rows)
                    self._populate_tree(self.kr_frame.sim_tree,   self.kr_db,
                                        [r for _, r in kr_res.similar_rows],
                                        scores=[s for s, _ in kr_res.similar_rows])
                    # US
                    self._populate_tree(self.us_frame.exact_tree, self.us_db, us_res.exact_rows)
                    self._populate_tree(self.us_frame.sim_tree,   self.us_db,
                                        [r for _, r in us_res.similar_rows],
                                        scores=[s for s, _ in us_res.similar_rows])
                    # EU
                    self._populate_tree(self.eu_frame.exact_tree, self.eu_db, eu_exact_compact)
                    self._populate_tree(self.eu_frame.sim_tree,   self.eu_db,
                                        [r for _, r in eu_res.similar_rows],
                                        scores=[s for s, _ in eu_res.similar_rows])

                    # 상태바
                    kr_status, kr_color = ("한국 사용 가능", "green") if kr_res.exact_rows else ("한국 사용 확인 필요", "red")
                    us_status, us_color = ("미국 사용 가능", "green") if us_res.exact_rows else ("미국 사용 확인 필요", "red")
                    eu_status, eu_color = ("유럽 사용 가능", "green") if eu_res.exact_rows else ("유럽 사용 확인 필요", "red")

                    self.status_bar.configure(state="normal")
                    self.status_bar.delete("1.0", tk.END)
                    self.status_bar.insert(tk.END, kr_status, ("kr",))
                    self.status_bar.insert(tk.END, " | ", ("sep",))
                    self.status_bar.insert(tk.END, us_status, ("us",))
                    self.status_bar.insert(tk.END, " | ", ("sep",))
                    self.status_bar.insert(tk.END, eu_status, ("eu",))
                    self.status_bar.tag_configure("kr", foreground=kr_color)
                    self.status_bar.tag_configure("sep", foreground="black")
                    self.status_bar.tag_configure("us", foreground=us_color)
                    self.status_bar.tag_configure("eu", foreground=eu_color)
                    self.status_bar.configure(state="disabled")

                    # 커서 원복
                    self.config(cursor="")

                self.after(0, apply_results)

            except Exception as e:
                err = str(e)
                self.after(0, lambda err=err: (self.config(cursor=""), messagebox.showerror("검색 오류", err)))


        threading.Thread(target=worker, daemon=True).start()

    def clear_results(self):
        for frame in (self.kr_frame, self.us_frame, self.eu_frame):
            for tree in (frame.exact_tree, frame.sim_tree):
                for i in tree.get_children():
                    tree.delete(i)
        self.status_bar.configure(state="normal")
        self.status_bar.delete("1.0", tk.END)
        self.status_bar.insert(tk.END, "상태 메시지: 검색어를 입력하세요.")
        self.status_bar.configure(state="disabled")

    def _truncate(self, s: Any) -> str:
        s = "" if s is None else str(s)
        s = s.replace("\n", " ").strip()
        return (s[:TRUNCATE_AT] + "…") if len(s) > TRUNCATE_AT else s

    def _populate_tree(self, tree: ttk.Treeview, db: ChemicalDB, rows: List[DBRow], scores: Optional[List[float]] = None):
        # 1) 기존 행 비우기
        for i in tree.get_children():
            tree.delete(i)

        # 2) 컬럼 구성
        cols = db.columns_to_display
        display_cols = [ROW_ID_COL] + (["_Score_"] if tree._show_score else []) + cols

        tree["columns"] = display_cols
        for c in display_cols:
            # 숨김 인덱스 컬럼 숨기기
            if c == ROW_ID_COL:
                tree.heading(c, text=ROW_ID_COL)
                tree.column(c, width=0, stretch=False)
                continue

            header_text = str(c)  # 기본은 원래 컬럼명

            # (A) 유사 검색 트리 점수 헤더: "_Score_" → "유사도 점수"
            if tree._show_score and c == "_Score_":
                header_text = "유사도 점수"

            # (B) KR 전용: '제품명'(B열)을 화면에서 '국문명'으로 표시
            if db.kind == "KR":
                if getattr(db, "korean_name_col", None) and c == db.korean_name_col:
                    header_text = "국문명"
                elif str(c).strip() == "제품명":  # 폴백
                    header_text = "국문명"

            # >>> 여기에 US 매핑을 추가하세요 <<<
            # (C) US 전용: CAS Reg. No. → "CAS No.", Substance → "영문명"
            if db.kind == "US":
                if getattr(db, "cas_col", None) and c == db.cas_col:
                    header_text = "CAS No."
                elif getattr(db, "primary_name_col", None) and c == db.primary_name_col:
                    header_text = "영문명"

            tree.heading(c, text=header_text)
            tree.column(c, width=150, anchor="w", stretch=True)


            # 헤더 라벨(유사도 점수/국문명 포함)
            is_score_col = (c == "_Score_")
            tree.heading(c, text=self._header_label(db, c, is_score_col))
            tree.column(c, width=150, anchor="w", stretch=True)

        # 3) 결과 없으면 placeholder
        if not rows:
            placeholder = [""] + (["(없음)"] + [""] * (len(display_cols) - 2))
            tree.insert("", "end", values=placeholder)
            self._autosize_tree_columns(tree)  # ← 추가
            return

        # 4) 데이터 렌더
        for idx, row in enumerate(rows):
            data = row.data
            values = [row.idx]  # __ROW_ID__ (숨김)
            if tree._show_score:
                sc = f"{scores[idx]:.0f}" if scores else ""
                values.append(sc)

            # 🔹 EU 테이블인 경우: 해당 행이 속한 CAS/이름 묶음에서
            #    '최상위(파일상 첫 행)'의 Used for / ML & Food notes를 미리 구함
            top_used, top_notes = None, None
            if db.kind == "EU":
                try:
                    _, key_value, pairs = self._eu_collect_pairs(data)  # (key_type, key_value, [(uf, notes), ...])
                    if pairs:
                        top_used, top_notes = pairs[0]
                except Exception:
                    top_used, top_notes = None, None

            for c in cols:
                cell_val = data.get(c, "")
                c_low = str(c).strip().lower()

                # EU 결과 트리에서는 Used for / ML & Food notes를 ‘최상위 1줄’만 표기
                if db.kind == "EU" and c_low in ("used for", "ml & food notes"):
                    if c_low == "used for" and top_used is not None:
                        values.append(self._truncate(top_used))
                        continue
                    if c_low == "ml & food notes" and top_notes is not None:
                        values.append(self._truncate(top_notes))
                        continue

                values.append(self._truncate(cell_val))

            tree.insert("", "end", values=values, tags=("row",))
        self._autosize_tree_columns(tree)

    # ------------------ AI Features ------------------
    def _start_summary_process_bulk(
        self,
        urls,
        summary_widget,
        chat_history_widget,
        chat_entry,
        chat_button,
        open_in_browser: bool = False,
        gauge: Optional[Tuple["ttk.Progressbar", "ttk.Label"]] = None
    ):
        # 필요시만 원문 탭을 띄우도록
        if open_in_browser:
            for u in urls:
                try:
                    webbrowser.open(u)
                except Exception:
                    pass

        if not GEMINI_CONFIGURED:
            messagebox.showerror("API 오류", "Gemini API가 설정되지 않았습니다. API 키를 확인하세요.")
            return

        # 게이지 초기화
        pb, lb = (None, None)
        if gauge and isinstance(gauge, tuple) and len(gauge) == 2:
            pb, lb = gauge
            # ★ 시작 시에는 indeterminate로 ‘돌리는’ 애니메이션 가동
            try:
                pb.stop()
                pb["mode"] = "indeterminate"
                pb["value"] = 0
                pb.start(12)  # 숫자 작을수록 빠름
            except Exception:
                pb = None
            if lb:
                lb.configure(text="원문 수집 준비 중…")


        th = threading.Thread(
            target=self._summarize_bulk_thread_target,
            args=(urls, summary_widget, chat_history_widget, chat_entry, chat_button, gauge),
            daemon=True
        )
        th.start()

    def _start_summary_process(self, url, summary_widget, chat_history_widget, chat_entry, chat_button):
        """단일 URL 요약 요청을 bulk 파이프라인으로 위임"""
        if not url:
            messagebox.showerror("AI 요약", "URL이 비어있습니다.")
            return
        self._start_summary_process_bulk(
            [url],
            summary_widget,
            chat_history_widget,
            chat_entry,
            chat_button,
            open_in_browser=False,
            gauge=None  # 단일 URL에선 게이지를 쓰지 않으니 None
    )


    def _summarize_bulk_thread_target(
        self,
        urls,
        summary_widget,
        chat_history_widget,
        chat_entry,
        chat_button,
        gauge: Optional[Tuple["ttk.Progressbar","ttk.Label"]] = None
    ):
        import re, time

        # 내부 헬퍼: 게이지 안전 업데이트
        def set_progress(p: float, msg: Optional[str] = None):
            try:
                if not gauge:
                    return
                pb, lb = gauge
                if pb:
                    # ★ 값 반영 전, indeterminate가 돌고 있으면 멈추고 determinate로 전환
                    def _apply(v):
                        try:
                            pb.stop()
                        except Exception:
                            pass
                        try:
                            pb["mode"] = "determinate"
                            pb.configure(value=max(0, min(100, v)))
                        except Exception:
                            pass
                    self.after(0, lambda v=p: _apply(v))
                if lb and msg is not None:
                    self.after(0, lambda m=msg: lb.configure(text=m))
            except Exception:
                pass


        def tick_towards(target: float, duration: float = 1.0, floor: float = 0.0):
            # 짧은 애니메이션(모델 대기 중 등) — UI 끊김 방지
            if not gauge:
                return
            pb, _ = gauge
            try:
                cur = float(pb["value"])
            except Exception:
                cur = floor
            steps = max(8, int(duration * 20))
            inc = max(0.0, (target - cur) / steps)
            for _ in range(steps):
                cur += inc
                self.after(0, lambda v=min(target, cur): pb.configure(value=v))
                time.sleep(max(0.01, duration / steps))

        def _is_ecfr(url: str) -> bool:
            u = (url or "").lower()
            return ("ecfr.gov" in u) or ("govinfo.gov" in u) or ("law.cornell.edu/cfr" in u) or ("/cfr/" in u)

        def _extract_meaningful_text_ecfr(soup: BeautifulSoup) -> str:
            for sel in ["nav", "header", "footer", "script", "style", "aside", ".global-header", ".global-footer", ".navigation"]:
                for el in soup.select(sel):
                    el.decompose()
            candidates = []
            for sel in [
                "div.content-block.leaf",
                "div#content div.section",
                "div#content article",
                "main .section",
                "main article",
                "div#content",
            ]:
                for el in soup.select(sel):
                    txt = el.get_text(" ", strip=True)
                    if txt and len(txt) > 120:
                        candidates.append(txt)
            base = max(candidates, key=len) if candidates else soup.get_text(" ", strip=True)

            keep_re = re.compile(
                r"\b(ppm|mg/kg|mg/L|percent|%|GMP|quantum\s+satis|not\s+more\s+than|no\s+more\s+than|"
                r"shall\s+not\s+exceed|max(imum)?\s+(level|amount)?|residue|limit|used\s+as|"
                r"for\s+use\s+as|permitted|safe\s+for\s+use)\b",
                re.I
            )
            lines = [ln.strip() for ln in re.split(r"[;\.\n]\s+", base) if ln.strip()]
            lines = [ln for ln in lines if keep_re.search(ln)]
            header_bits = []
            for hsel in ["h1", "h2", ".hierarchyTitle", ".part-heading", ".section-head"]:
                for el in soup.select(hsel):
                    t = el.get_text(" ", strip=True)
                    if t:
                        header_bits.append(t)
            headers = " | ".join(dict.fromkeys(header_bits))[:400]
            filtered = "\n".join(dict.fromkeys(lines))
            return (headers + "\n" + filtered).strip()

        try:
            if not urls:
                raise ValueError("요약할 CFR 링크가 없습니다.")

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

            # ① 원문 수집 단계 (0 → 60%)
            set_progress(3, "원문 수집 중…")
            parts = []
            n = max(1, len(urls))
            for i, u in enumerate(urls, start=1):
                try:
                    resp = requests.get(u, timeout=20, headers=headers)
                    resp.raise_for_status()
                    soup = BeautifulSoup(resp.content, "html.parser")
                    if _is_ecfr(u):
                        text = _extract_meaningful_text_ecfr(soup)
                    else:
                        main = (
                            soup.select_one("div.content-block.leaf") or
                            soup.select_one("main") or
                            soup.select_one("article") or
                            soup.select_one("div#content") or
                            soup.body
                        )
                        raw = main.get_text(" ", strip=True) if main else soup.get_text(" ", strip=True)
                        raw = re.sub(r"\s+", " ", raw).strip()
                        keep = []
                        for ln in re.split(r"[;\.\n]\s+", raw):
                            ln = ln.strip()
                            if not ln:
                                continue
                            if re.search(r"\b(ppm|mg/kg|mg/L|%|percent|GMP|quantum\s+satis|not\s+more\s+than|"
                                        r"shall\s+not\s+exceed|max|limit|residue|used\s+as)\b", ln, re.I):
                                keep.append(ln)
                        text = "\n".join(dict.fromkeys(keep)) or raw[:4000]
                    if text:
                        parts.append(f"[SOURCE] {u}\n{text}")
                    else:
                        parts.append(f"[SOURCE] {u}\n(의미있는 텍스트를 찾지 못함)")
                except Exception as e:
                    parts.append(f"[SOURCE] {u}\n(수집 실패: {e})")

                # 진행률 업데이트
                pct = 3 + (57.0 * i / n)  # 3%→60%
                set_progress(pct, f"원문 수집 {i}/{n}")

            combined_source = "\n\n---\n\n".join(parts)
            if not combined_source.strip():
                raise ValueError("CFR 원문을 추출하지 못했습니다.")

            # ② 모델 요약 단계 (60 → 95%)
            set_progress(62, "AI 요약 중…")
            # 대기 애니메이션(스트리밍 대용)
            threading.Thread(target=lambda: tick_towards(92, duration=1.4, floor=62), daemon=True).start()

            prompt = (
                "아래는 CFR(연방규정) 섹션들에서 추출·정리한 본문 발췌입니다. "
                "탐색/검색 안내문 등 비규제성 문구는 무시하고, 다음 항목만 한국어로 간결하게 정리하세요.\n"
                "1) 섹션 번호/제목, 2) 용도(Used as/For), 3) 허용 한도(수치: %, mg/kg, mg/L, ppm, GMP/quantum satis 등), "
                "4) 제한/예외, 5) 주의·비고. 수치가 명시된 문장만 우선합니다.\n"
                "- 동일/유사 규정은 병합하고, 상충 시 둘 다 표기하며 각 항목 끝에 (출처: URL) 붙이세요.\n"
                "- 결과는 표 형태 없이 불릿 리스트로 번호를 매겨 주세요.\n\n"
                f"{combined_source[:120000]}\n"
            )
            with _silence_stderr_c():
                answer = GEMINI_MODEL.generate_content(prompt).text

            set_progress(95, "UI 반영 중…")

            # ③ UI 반영 및 완료 (95 → 100%)
            def on_ok():
                summary_widget['state'] = 'normal'
                summary_widget.delete('1.0', tk.END)
                summary_widget.insert('1.0', answer)
                summary_widget['state'] = 'disabled'

                chat_history_widget.initial_context = (
                    "아래는 다수 CFR 페이지에서 추출된 합본 원문입니다. "
                    "이 텍스트만을 근거로 간단명료하게 한국어로 답변하세요.\n\n"
                    + combined_source[:120000]
                )
                chat_entry['state'] = 'normal'
                chat_button['state'] = 'normal'

                set_progress(100, "요약 완료")
            self.after(0, on_ok)

        except Exception as e:
            def on_fail():
                summary_widget['state'] = 'normal'
                summary_widget.delete('1.0', tk.END)
                summary_widget.insert('1.0', f"통합 요약 실패: {e}")
                summary_widget['state'] = 'disabled'
                set_progress(0, "실패")
            self.after(0, on_fail)

    def _update_ui_with_summary(self, summary, summary_widget, chat_history_widget, chat_entry, chat_button):
        summary_widget['state'] = 'normal'
        summary_widget.delete('1.0', tk.END)
        summary_widget.insert('1.0', summary)
        summary_widget['state'] = 'disabled'

        chat_history_widget.initial_context = summary
        chat_history_widget['state'] = 'normal'
        chat_history_widget.delete('1.0', tk.END)
        chat_history_widget.insert(tk.END, "요약 내용에 대해 질문하세요.\n\n", ("info",))
        chat_history_widget.tag_configure("info", foreground="blue")
        chat_history_widget['state'] = 'disabled'

        chat_entry['state'] = 'normal'
        chat_button['state'] = 'normal'

    def _update_ui_with_error(self, error_message, summary_widget):
        summary_widget['state'] = 'normal'
        summary_widget.delete('1.0', tk.END)
        summary_widget.insert('1.0', error_message)
        summary_widget['state'] = 'disabled'

    def _handle_chat_submit(self, chat_history_widget, chat_entry):
        question = chat_entry.get().strip()
        if not question:
            return
        chat_entry.delete(0, tk.END)
        chat_history_widget['state'] = 'normal'
        chat_history_widget.insert(tk.END, f"Q: {question}\n", ("user",))
        chat_history_widget.tag_configure("user", foreground="black", font=("", 10, "bold"))
        chat_history_widget.insert(tk.END, "A: 답변 생성 중...\n", ("thinking",))
        chat_history_widget.tag_configure("thinking", foreground="gray")
        chat_history_widget.see(tk.END)
        chat_history_widget['state'] = 'disabled'
        thread = threading.Thread(target=self._chat_thread_target, args=(question, chat_history_widget))
        thread.daemon = True
        thread.start()

    def _chat_thread_target(self, question, chat_history_widget):
        try:
            initial_context = getattr(chat_history_widget, 'initial_context', 'No context available.')
            prompt = (
                "다음 컨텍스트를 근거로 간단명료하게 한국어로 답해주세요.\n\n"
                f"{initial_context}\n\n"
                f"질문: {question}"
            )
            response = GEMINI_MODEL.generate_content(prompt).text
            self.after(0, self._update_chat_ui, response, chat_history_widget)
        except Exception as e:
            error_message = f"API 오류: {e}"
            self.after(0, self._update_chat_ui, error_message, chat_history_widget)

    def _update_chat_ui(self, response, chat_history_widget):
        chat_history_widget['state'] = 'normal'
        chat_history_widget.delete("end-2l", "end-1l") 
        chat_history_widget.insert(tk.END, f"A: {response}\n\n", ("bot",))
        chat_history_widget.tag_configure("bot", foreground="#007A70")
        chat_history_widget.see(tk.END)
        chat_history_widget['state'] = 'disabled'

    # ---------- EU: 동일 CAS/Name 묶음 수집 & 요약 ----------
    def _eu_collect_pairs(self, row_dict: dict) -> Tuple[str, str, List[Tuple[str, str]]]:
        """행 하나를 기준으로, 같은 CAS(우선) 또는 같은 Additive_Name_EN의
        모든 (Used for, ML & Food notes) 쌍을 원본 DataFrame 순서대로 모아 반환.
        return: (key_type, key_value, pairs)
        """
        def get_by_keys(d: dict, candidates: List[str]) -> str:
            low = {str(k).strip().lower(): k for k in d.keys()}
            for cand in candidates:
                key = cand.strip().lower()
                if key in low:
                    return str(d.get(low[key], "") or "").strip()
            return ""

        # 그룹 키 결정(CAS 우선)
        for cand in ["cas_list","cas list","cas reg. no.","cas reg no","cas reg no.","cas","cas no.","cas no"]:
            v = get_by_keys(row_dict, [cand])
            if v:
                m = CAS_PATTERN.search(v)
                if m:
                    key_type, key_value = "CAS", m.group(0)
                    break
        else:
            key_type = key_value = ""
            for cand in ["additive_name_en", "additive name en", "name_en", "name en"]:
                v = get_by_keys(row_dict, [cand])
                if v:
                    key_type, key_value = "NAME", v
                    break

        pairs: List[Tuple[str, str]] = []
        df_eu = self.eu_db.df if (self.eu_db and self.eu_db.df is not None) else None
        if df_eu is None or not key_type:
            return key_type, key_value, pairs

        def row_matches(r: "pd.Series") -> bool:
            idx_map = {str(c).strip().lower(): c for c in r.index}
            if key_type == "CAS":
                for cand in ["cas_list","cas list","cas reg. no.","cas reg no","cas reg no.","cas","cas no.","cas no"]:
                    if cand in idx_map:
                        cas_cell = str(r.get(idx_map[cand], "") or "")
                        return bool(re.search(rf"\b{re.escape(key_value)}\b", cas_cell))
                return False
            else:
                for cand in ["additive_name_en", "additive name en", "name_en", "name en"]:
                    if cand in idx_map:
                        return str(r.get(idx_map[cand], "") or "").strip() == key_value
                return False

        for _, r in df_eu.iterrows():
            if not row_matches(r):
                continue
            idx_map = {str(c).strip().lower(): c for c in r.index}
            uf = str(r.get(idx_map.get("used for", ""), "") or "").strip()
            notes = ""
            for mk in ["ml & food notes", "ml & food notes"]:
                if mk in idx_map:
                    notes = str(r.get(idx_map[mk], "") or "").strip()
                    break
            if uf or notes:
                pairs.append((uf, notes))

        # 중복 제거(원본 순서 유지)
        seen = set(); uniq = []
        for a, b in pairs:
            k = (a.strip(), b.strip())
            if k not in seen:
                seen.add(k); uniq.append(k)
        return key_type, key_value, uniq

    def _eu_pairs_as_text(self, cas_or_name: str, pairs: List[Tuple[str, str]]) -> str:
        """AI 베이스용 간단 표 텍스트 생성"""
        lines = [f"[EU] 기준 정보 — 키: {cas_or_name}", "Used for\tML & Food notes"]
        for uf, mn in pairs:
            lines.append(f"{uf}\t{mn}")
        return "\n".join(lines)

    # ---------- EU: 정확일치 결과 1건만 남기기 ----------
    def _eu_dedupe_exact(self, rows: List[DBRow]) -> List[DBRow]:
        """EU exact_rows에서 같은 CAS(우선) 또는 같은 Additive_Name_EN 그룹의
        '첫 번째(원본 엑셀 상으로 가장 위)' 행만 남긴다."""
        out: List[DBRow] = []
        seen_keys = set()
        for r in rows:
            try:
                _, key_value, _ = self._eu_collect_pairs(r.data)
                key = key_value.strip() if key_value else ""
            except Exception:
                key = ""

            if not key:
                key = str(r.data.get("Substance", "")).strip()

            if key not in seen_keys:
                seen_keys.add(key)
                out.append(r)
        return out

    # ------------------ Detail Popup ------------------
    def open_detail_popup(self, tree: ttk.Treeview):
        sel = tree.selection()
        if not sel:
            return
        item_id = sel[0]

        # ✅ 트리에 심어둔 DB 종류(kind)
        kind = getattr(tree, "_db_kind", None)
        if kind == "US":
            db = self.us_db
            is_eu = False
        elif kind == "EU":
            db = self.eu_db
            is_eu = True
        else:
            db = self.kr_db
            is_eu = False

        # 숨김 인덱스로 원본 행 조회
        try:
            row_idx = int(tree.set(item_id, ROW_ID_COL))
        except Exception:
            row_idx = None

        if row_idx is not None and db and db.df is not None and 0 <= row_idx < len(db.df):
            full_row = db.df.iloc[row_idx].to_dict()
        else:
            full_row = {c: tree.set(item_id, c) for c in tree["columns"]}

        win = tk.Toplevel(self)
        win.title("상세보기")

        container = ttk.Frame(win, padding=10)
        container.pack(fill=tk.BOTH, expand=True)

        canvas = tk.Canvas(container)
        vsb = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        frame = ttk.Frame(canvas)

        frame.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=frame, anchor="nw")
        canvas.configure(yscrollcommand=vsb.set)

        # --- 마우스 휠로 스크롤 가능하게 ---
        def _on_mousewheel(evt):
            if evt.delta:  # Windows/macOS
                canvas.yview_scroll(-int(evt.delta/120), "units")

        canvas.bind_all("<MouseWheel>", _on_mousewheel)                         # Win/macOS
        canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))  # Linux
        canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll( 1, "units"))

        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        
        # --- Summary Text Area ---
        summary_text = tk.Text(frame, height=24, wrap="word", relief=tk.SUNKEN, borderwidth=1, state="disabled", foreground="gray")
        # --- Chat History Area ---
        chat_history = tk.Text(frame, height=15, wrap="word", relief=tk.SUNKEN, borderwidth=1, state="disabled")

        # --- Chat Input Frame ---
        chat_input_frame = ttk.Frame(frame)
        chat_input_frame.columnconfigure(0, weight=1)
        chat_entry = ttk.Entry(chat_input_frame, state="disabled")
        chat_entry.grid(row=0, column=0, sticky='ew')
        chat_btn = ttk.Button(chat_input_frame, text="질문하기", state="disabled",
                            command=lambda: self._handle_chat_submit(chat_history, chat_entry))
        chat_btn.grid(row=0, column=1, sticky='e', padx=(8, 0))
        chat_entry.bind("<Return>", lambda event: self._handle_chat_submit(chat_history, chat_entry))

        # ------------------------------------------------------------
        # ★ EU 전용 표 렌더링 (기존 유지) + KR 전용 분기 추가
        # ------------------------------------------------------------
        current_row = 0
        base_text = None  # EU에서만 사용

        if is_eu:
            # (기존 EU 표 렌더 블록 그대로)
            key_type, key_value, pairs = self._eu_collect_pairs(full_row)

            table_frame = ttk.Frame(frame)
            table_frame.grid(row=current_row, column=0, columnspan=2, sticky="nsew", pady=(0, 8))

            canvas_table = tk.Canvas(table_frame, highlightthickness=0)
            v_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=canvas_table.yview)
            h_scroll = ttk.Scrollbar(table_frame, orient="horizontal", command=canvas_table.xview)
            canvas_table.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
            canvas_table.grid(row=0, column=0, sticky="nsew")
            v_scroll.grid(row=0, column=1, sticky="ns")
            h_scroll.grid(row=1, column=0, sticky="ew")
            table_frame.grid_rowconfigure(0, weight=1)
            table_frame.grid_columnconfigure(0, weight=1)

            inner = ttk.Frame(canvas_table)
            canvas_table.create_window((0, 0), window=inner, anchor="nw")
            inner.bind("<Configure>", lambda e: canvas_table.configure(scrollregion=canvas_table.bbox("all")))

            header_kwargs = {"font": ("", 10, "bold")}
            ttk.Label(inner, text=f"CAS/Name: {key_value}", **header_kwargs).grid(row=0, column=0, columnspan=2, sticky="w", padx=4, pady=(4, 0))
            ttk.Label(inner, text="Used for", **header_kwargs).grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
            ttk.Label(inner, text="ML & Food notes", **header_kwargs).grid(row=1, column=1, sticky="nsew", padx=4, pady=4)

            start_row = 2
            if not pairs:
                ttk.Label(inner, text="(표시할 항목이 없습니다)", foreground="gray").grid(row=start_row, column=0, columnspan=2, sticky="w", padx=6, pady=6)
            else:
                def calc_h(txt: str, chars_per_line: int = 42, max_lines: int = 10) -> int:
                    if not txt: return 1
                    est = max(1, (len(txt) // chars_per_line) + (1 if len(txt) % chars_per_line else 0))
                    return min(max_lines, est)

                for i, (uf, mn) in enumerate(pairs, start=start_row):
                    h1, h2 = calc_h(uf), calc_h(mn)
                    uf_txt = tk.Text(inner, wrap="word", height=h1, width=46, relief="solid", bd=1)
                    uf_txt.insert("1.0", uf); uf_txt.configure(state="disabled")
                    uf_txt.grid(row=i, column=0, sticky="nsew", padx=4, pady=2)

                    mn_txt = tk.Text(inner, wrap="word", height=h2, width=46, relief="solid", bd=1)
                    mn_txt.insert("1.0", mn); mn_txt.configure(state="disabled")
                    mn_txt.grid(row=i, column=1, sticky="nsew", padx=4, pady=2)

                inner.grid_columnconfigure(0, weight=1)
                inner.grid_columnconfigure(1, weight=1)

            current_row += 1
            sep0 = ttk.Separator(frame, orient='horizontal')
            sep0.grid(row=current_row, column=0, columnspan=2, sticky='ew', pady=(4, 10))
            current_row += 1

            base_text = self._eu_pairs_as_text(key_value or "N/A", pairs)

        elif kind == "KR":
            # 상세 렌더 + '사용기준' 원문 확보
            current_row, usage_context = self._render_kr_detail(
                frame, full_row, self.kr_db,
                summary_text, chat_history, chat_entry, chat_btn
            )

            # 상단 큰 요약(Text) 박스는 제거
            try:
                summary_text.destroy()
            except Exception:
                pass

            # --- 하단 Gemini 요약/채팅 UI 배치 (US 전용) ---
            ai_row = current_row
            sep = ttk.Separator(frame, orient='horizontal')
            sep.grid(row=ai_row, column=0, columnspan=2, sticky='ew', pady=15)

            # ★ 헤더 + 게이지(Progressbar) + 상태문구
            header_row = ttk.Frame(frame)
            header_row.grid(row=ai_row + 1, column=0, columnspan=2, sticky='we', pady=(0, 5))
            header_row.grid_columnconfigure(0, weight=0)
            header_row.grid_columnconfigure(1, weight=0)
            header_row.grid_columnconfigure(2, weight=1)

            ttk.Label(header_row, text="Gemini AI 요약 및 채팅 (CFR 통합)", font=("", 11, "bold"))\
                .grid(row=0, column=0, sticky='w')

            cfr_pb = ttk.Progressbar(header_row, mode="determinate", maximum=100, length=180)
            cfr_pb.grid(row=0, column=1, padx=(10, 8), sticky='w')
            cfr_status = ttk.Label(header_row, text="", foreground="gray")
            cfr_status.grid(row=0, column=2, sticky='w')

            # ★ 추가: 최초에도 ‘빈 막대’가 보이도록 1% 채워둠 + 최소 높이 확보
            try:
                cfr_pb["value"] = 1
            except Exception:
                pass
            header_row.update_idletasks()

            # 요약 영역 + 채팅 영역
            summary_text.grid(row=ai_row + 2, column=0, columnspan=2, sticky='nsew', pady=(0, 6))
            chat_history.grid(row=ai_row + 3, column=0, columnspan=2, sticky='nsew', pady=4)
            chat_input_frame.grid(row=ai_row + 4, column=0, columnspan=2, sticky='ew')
            frame.grid_rowconfigure(ai_row + 2, weight=1)
            frame.grid_rowconfigure(ai_row + 3, weight=1)

            chat_history.grid(row=ai_row + 2, column=0, columnspan=2, sticky='nsew', pady=4)
            chat_input_frame.grid(row=ai_row + 3, column=0, columnspan=2, sticky='ew')
            frame.grid_rowconfigure(ai_row + 2, weight=1)

            # 초기 메시지
            chat_history.configure(state="normal")
            chat_history.delete("1.0", tk.END)
            chat_history.insert(tk.END, "해당 첨가물의 사용 정보에 대한 질문이 가능합니다.\n\n")
            chat_history.configure(state="disabled")

            # Gemini 컨텍스트 = '사용기준' 원문
            chat_history.initial_context = (
                "다음은 특정 식품첨가물의 '사용정보(사용기준)' 전문입니다. "
                "이 텍스트만을 근거로 간단명료하게 한국어로 답변하세요.\n\n" + (usage_context or "(내용 없음)")
            )

            # 입력 활성화
            if GEMINI_CONFIGURED:
                chat_entry.configure(state="normal")
                chat_btn.configure(state="normal")
            else:
                chat_history.configure(state="normal")
                chat_history.insert(tk.END, "※ 현재 Gemini API 키가 설정되지 않아 답변 생성이 비활성화되어 있습니다.\n", ("info",))
                chat_history.tag_configure("info", foreground="blue")
                chat_history.configure(state="disabled")


        elif kind == "US":
            # === US 상세: CFR 헤더/게이지를 '먼저' 만들어 두기 ===
            header_row = ttk.Frame(frame)
            header_row.grid_columnconfigure(0, weight=0)
            header_row.grid_columnconfigure(1, weight=0)
            header_row.grid_columnconfigure(2, weight=1)

            ai_header_lbl = ttk.Label(header_row, text="Gemini AI 요약 및 채팅 (CFR 통합)", font=("", 11, "bold"))
            cfr_pb = ttk.Progressbar(header_row, mode="determinate", maximum=100, length=180)
            cfr_status = ttk.Label(header_row, text="", foreground="gray")

            # ★ US 상세 렌더: 모든 필드 먼저 표시하면서 URL 수집
            def _is_cfr(u: str) -> bool:
                u = (u or "").lower()
                return ("ecfr.gov" in u) or ("govinfo.gov" in u) or ("law.cornell.edu/cfr" in u) or ("/cfr/" in u)

            row_index = current_row - 1
            all_urls = []
            for i, (k, v) in enumerate(full_row.items(), start=current_row):
                if str(k).strip() == ROW_ID_COL:
                    continue

                row_index = i
                key_lbl = ttk.Label(frame, text=str(k), font=("", 10, "bold"))
                key_lbl.grid(row=i, column=0, sticky="ne", padx=(0, 8), pady=4)

                val_frame = ttk.Frame(frame)
                val_frame.grid(row=i, column=1, sticky="we", pady=4)
                frame.grid_columnconfigure(1, weight=1)

                text = str(v) if v is not None else ""
                value_lbl = tk.Text(
                    val_frame,
                    height=min(6, max(1, (len(text) // 80) + 1)),
                    wrap="word", relief="flat", background=win.cget('bg')
                )
                value_lbl.insert("1.0", text)
                value_lbl.configure(state="disabled")
                value_lbl.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

                # URL 추출: 통합 요약용 수집 + CFR 줄에만 '통합 버튼 2개' 노출
                urls = extract_urls(text)
                if urls:
                    # 전체 수집(필요시 다른 용도)
                    all_urls.extend(urls)

                # 이 행이 'CFR' 필드라면, 이 줄 바로 아래에 버튼 2개를 붙인다
                key_lower = str(k).strip().lower()
                if "cfr" in key_lower:
                    # CFR만 필터 + 중복 제거
                    seen = set()
                    cfr_urls = []
                    for u in urls:
                        if _is_cfr(u) and u not in seen:
                            seen.add(u)
                            cfr_urls.append(u)

                    btn_row_inline = ttk.Frame(val_frame, padding=(6, 2))
                    btn_row_inline.pack(side=tk.LEFT, anchor="n")

                    ttk.Button(
                        btn_row_inline,
                        text=f"관련 원문 AI 요약 (CFR {len(cfr_urls)}개)",
                        command=lambda urls=cfr_urls: self._start_summary_process_bulk(
                            urls, summary_text, chat_history, chat_entry, chat_btn,
                            open_in_browser=False,
                            gauge=(cfr_pb, cfr_status)   # ★ 게이지/상태 라벨 전달
                        )
                    ).pack(side="left")


                    ttk.Button(
                        btn_row_inline,
                        text="원문 모두 열기(선택)",
                        command=lambda urls=cfr_urls: [webbrowser.open(u) for u in urls]
                    ).pack(side="left", padx=(8, 0))


            current_row = row_index + 1

            # CFR 링크만 필터링
            def _is_cfr(u: str) -> bool:
                u = (u or "").lower()
                return ("ecfr.gov" in u) or ("govinfo.gov" in u) or ("law.cornell.edu/cfr" in u) or ("/cfr/" in u)

            cfr_urls = []
            seen = set()
            for u in all_urls:
                if _is_cfr(u) and u not in seen:
                    seen.add(u)
                    cfr_urls.append(u)

            current_row = row_index + 1  # 루프 끝나고 높이 산정

            # --- 하단 Gemini 요약/채팅 UI 배치 (US 전용) ---
            ai_row = current_row
            sep = ttk.Separator(frame, orient='horizontal')
            sep.grid(row=ai_row, column=0, columnspan=2, sticky='ew', pady=15)

            # 헤더/게이지 배치
            ai_header_lbl.grid(row=0, column=0, sticky='w')
            cfr_pb.grid(row=0, column=1, padx=(10, 8), sticky='w')
            cfr_status.grid(row=0, column=2, sticky='w')
            header_row.grid(row=ai_row + 1, column=0, columnspan=2, sticky='we')

            # 요약/채팅 영역 배치 (중복 없이 한 번만)
            summary_text.grid(row=ai_row + 2, column=0, columnspan=2, sticky='nsew', pady=(0, 6))
            chat_history.grid(row=ai_row + 3, column=0, columnspan=2, sticky='nsew', pady=4)
            chat_input_frame.grid(row=ai_row + 4, column=0, columnspan=2, sticky='ew')
            frame.grid_rowconfigure(ai_row + 2, weight=1)
            frame.grid_rowconfigure(ai_row + 3, weight=1)

            # 채팅 안내
            chat_history.configure(state="normal")
            chat_history.delete("1.0", tk.END)
            if cfr_urls:
                chat_history.insert(tk.END, f"CFR 원문 {len(cfr_urls)}개를 통합 요약 후 질문할 수 있습니다.\n\n")
            else:
                chat_history.insert(tk.END, "CFR 링크를 찾지 못했습니다. 그래도 질문은 가능합니다.\n\n")
            chat_history.configure(state="disabled")

            if GEMINI_CONFIGURED:
                chat_entry.configure(state="normal")
                chat_btn.configure(state="normal")
            else:
                chat_history.configure(state="normal")
                chat_history.insert(tk.END, "※ 현재 Gemini API 키가 설정되지 않아 답변 생성이 비활성화되어 있습니다.\n", ("info",))
                chat_history.tag_configure("info", foreground="blue")
                chat_history.configure(state="disabled")

        else:
            # ★ (US 제외) 기존 공통 렌더 유지 — 네 코드의 '기존 else' 내용을 여기로 둬
            row_index = current_row - 1
            for i, (k, v) in enumerate(full_row.items(), start=current_row):
                if str(k).strip() == ROW_ID_COL:
                    continue

                row_index = i
                key_lbl = ttk.Label(frame, text=str(k), font=("", 10, "bold"))
                key_lbl.grid(row=i, column=0, sticky="ne", padx=(0, 8), pady=4)

                val_frame = ttk.Frame(frame)
                val_frame.grid(row=i, column=1, sticky="we", pady=4)
                frame.grid_columnconfigure(1, weight=1)

                text = str(v) if v is not None else ""
                value_lbl = tk.Text(val_frame, height=min(6, max(1, (len(text) // 80) + 1)),
                                    wrap="word", relief="flat", background=win.cget('bg'))
                value_lbl.insert("1.0", text)
                value_lbl.configure(state="disabled")
                value_lbl.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

                urls = extract_urls(text)
                if urls:
                    link_frame = ttk.Frame(val_frame, padding=(6, 0))
                    link_frame.pack(side=tk.LEFT)
                    for u in urls:
                        btn = ttk.Button(
                            link_frame, text="AI 요약",
                            command=lambda url=u, st=summary_text, ch=chat_history, ce=chat_entry, cb=chat_btn:
                                self._start_summary_process(url, st, ch, ce, cb)
                        )
                        btn.pack(anchor="w", pady=2)

            current_row = row_index + 1


    # 아래는 AI 요약/채팅 영역 (분기별로 계산된 current_row를 기준으로 배치)
        # ↓ KR은 위에서 이미 AI 영역을 그렸으므로, 공통 블록은 KR 외 국가만 적용
        if kind not in ("KR", "US"):
            ai_row = current_row

            separator = ttk.Separator(frame, orient='horizontal')
            separator.grid(row=ai_row, column=0, columnspan=2, sticky='ew', pady=15)

            ai_header_lbl = ttk.Label(frame, text="Gemini AI 요약 및 채팅", font=("", 11, "bold"))
            ai_header_lbl.grid(row=ai_row + 1, column=0, columnspan=2, sticky='w', pady=(0, 5))

            summary_text.grid(row=ai_row + 2, column=0, columnspan=2, sticky='nsew', pady=4)
            chat_history.grid(row=ai_row + 3, column=0, columnspan=2, sticky='nsew', pady=4)
            chat_input_frame.grid(row=ai_row + 4, column=0, columnspan=2, sticky='ew')

            frame.grid_rowconfigure(ai_row + 2, weight=1)
            frame.grid_rowconfigure(ai_row + 3, weight=1)

            if is_eu and 'base_text' in locals():
                summary_text.configure(state="normal")
                summary_text.delete("1.0", tk.END)
                summary_text.insert("1.0", base_text)
                summary_text.configure(state="disabled")

                chat_history.initial_context = base_text
                chat_history.configure(state="normal")
                chat_history.delete("1.0", tk.END)

                if GEMINI_CONFIGURED:
                    chat_history.insert(tk.END, "표 기반으로 질문을 입력하세요.\n\n", ("info",))
                    chat_entry.configure(state="normal")
                    chat_btn.configure(state="normal")
                else:
                    chat_history.insert(
                        tk.END,
                        "표는 준비됐지만 Gemini API 키가 없어 질문 기능이 비활성화되어 있습니다.\n"
                        "환경변수 GOOGLE_API_KEY 를 설정하거나 코드에 API 키를 지정한 뒤 다시 실행하세요.\n\n",
                        ("info",)
                    )
                chat_history.tag_configure("info", foreground="blue")
                chat_history.configure(state="disabled")
            else:
                summary_text.insert("1.0", "요약할 링크의 'AI 요약' 버튼을 클릭하세요...")
 
        # --- [교체] KR/US 상세 팝업 자동 크기 조정(우측/하단 클리핑 방지, 초기 가독 높이 보장) ---
        try:
            if kind in ("KR", "US"):
                # 레이아웃 계산 갱신(두 번 호출로 측정 안정화)
                win.update_idletasks()
                frame.update_idletasks()

                # 1) 스크롤바/테두리/타이틀바 패딩 산정
                #    - 수직 스크롤바 두께(테마별 편차 고려, 최소 18px 확보)
                try:
                    sb_w = int(vsb.winfo_width())
                    if sb_w < 12:
                        sb_w = 18
                except Exception:
                    sb_w = 18

                #    - 윈도우 테두리/그림자 여유
                border_pad = 36
                #    - 타이틀바 높이(플랫폼별 편차 → 보수적으로 52px)
                title_pad = 64
                #    - 하단 여유(스크롤바/버튼/그림자 여지)
                bottom_pad = 44

                # 2) '내용이 한눈에' 원칙: 요청 폭/높이 + 여유
                req_w = frame.winfo_reqwidth()  + sb_w + border_pad
                req_h = frame.winfo_reqheight() + title_pad + bottom_pad

                # 3) 화면 한계(초기 팝업이 화면을 넘지 않도록 캡)
                screen_w = win.winfo_screenwidth()
                screen_h = win.winfo_screenheight()
                max_w = int(screen_w * 0.95)
                max_h = int(screen_h * 0.94)  # ← 높이 캡을 살짝 완화(90%→94%)

                # 4) 최소 가독 크기(높이 상향)
                min_w = 760
                min_h = 700

                final_w = max(min_w, min(req_w, max_w))
                final_h = max(min_h, min(req_h, max_h))

                # 5) 화면 중앙 정렬
                pos_x = (screen_w - final_w) // 2
                pos_y = (screen_h - final_h) // 2

                win.geometry(f"{final_w}x{final_h}+{pos_x}+{pos_y}")
                win.minsize(min_w, min_h)

                # 측정 후 마지막으로 한 번 더 갱신(드문 케이스 클리핑 방지)
                win.update_idletasks()
        except Exception:
            pass

    # ---------- EU 그룹 추출 팝업 ----------
    def _eu_group_extract(self, url: str):
        def worker():
            try:
                rows = parse_eu_group_page(url)
                self.after(0, lambda: self._show_eu_rows_popup(url, rows))
            except Exception as e:
                self.after(0, lambda: messagebox.showerror("EU 그룹 추출 오류", str(e)))
        th = threading.Thread(target=worker, daemon=True)
        th.start()

    def _show_eu_rows_popup(self, src_url: str, rows: List[Dict[str, str]]):
        win = tk.Toplevel(self)
        win.title("EU 그룹 추출 결과 (7필드)")
        win.geometry("1200x600")

        cols = ["E_Number","Additive_Name_EN","Synonyms","CAS_List",
                "Food category","Individual restriction(s) / exception(s)","Footnotes"]
        tree = ttk.Treeview(win, columns=cols, show="headings")
        for c in cols:
            tree.heading(c, text=c)
            tree.column(c, width=180 if c in cols[:4] else 420, anchor="w")

        vsb = ttk.Scrollbar(win, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)

        for r in rows:
            tree.insert("", "end", values=[r.get(c,"") for c in cols])

        btns = ttk.Frame(win); btns.pack(fill="x", padx=8, pady=8)
        def copy_clip():
            lines = ["\t".join(cols)]
            for r in rows:
                lines.append("\t".join(r.get(c,"") for c in cols))
            self.clipboard_clear()
            self.clipboard_append("\n".join(lines))
            messagebox.showinfo("복사 완료", "표가 클립보드로 복사되었습니다.")
        def save_excel():
            try:
                import pandas as pd
            except Exception:
                messagebox.showwarning("pandas 필요", "pandas가 필요합니다: pip install pandas")
                return
            path = filedialog.asksaveasfilename(
                title="EU_그룹추출_저장",
                defaultextension=".xlsx",
                filetypes=[("Excel","*.xlsx")])
            if not path:
                return
            df = pd.DataFrame(rows)
            df.to_excel(path, index=False)
            messagebox.showinfo("저장 완료", f"파일이 저장되었습니다.\n{path}")
        ttk.Button(btns, text="클립보드 복사", command=copy_clip).pack(side="left")
        ttk.Button(btns, text="엑셀로 저장", command=save_excel).pack(side="left", padx=6)
        ttk.Label(btns, text=_clean(src_url), foreground="gray").pack(side="right")

# ---------------- Main ----------------
def main():
    if _MISSING_DEPS:
        print("Warning: missing dependencies ->", ", ".join(_MISSING_DEPS), file=sys.stderr)
    app = App()
    app.mainloop()

if __name__ == "__main__":
    main()

# ====================== Web (Streamlit) Adapter ======================
# 이 블록을 파일 맨 아래 'if __name__ == "__main__": main()' 위/아래 어디든 추가해도 됩니다.
# 단, 중복 정의/호출을 피하려면 'run_streamlit()'만 추가하고, 실행 분기는 파일 맨 마지막에 둡니다.

def run_streamlit():
    try:
        import streamlit as st
    except Exception as e:
        print("[WEB] Streamlit이 설치되어 있지 않습니다: pip install streamlit")
        raise

    import io
    import base64

    st.set_page_config(page_title="SEMIPRO | Sempio Additives", layout="wide")

    # ----- 상단 CI + 제목 -----
    st.markdown(
        """
        <div style="display:flex;align-items:center;gap:14px;margin:6px 0 14px 0">
            <span style="font-weight:800;font-size:28px;">SEMIPRO</span>
            <span style="opacity:0.7;">Sempio Multi-market Additives</span>
        </div>
        """,
        unsafe_allow_html=True
    )
    st.write("---")

    # ----- 사이드바: 파일 선택 / 업로드 -----
    st.sidebar.header("데이터 파일")
    st.sidebar.caption("로컬 경로가 아닌, 웹에선 업로드/리포지토리 파일을 사용하세요.")

    # 1) 업로드 사용(권장)
    kr_up = st.sidebar.file_uploader("KR 엑셀 업로드", type=["xlsx", "xls"], key="up_kr")
    us_up = st.sidebar.file_uploader("US 엑셀 업로드", type=["xlsx", "xls"], key="up_us")
    eu_up = st.sidebar.file_uploader("EU 엑셀 업로드", type=["xlsx", "xls"], key="up_eu")

    # 2) 레포지토리에 같이 올린 기본 파일 사용(선택)
    st.sidebar.write("또는 저장소 기본 파일 경로를 입력(있을 때만):")
    kr_repo = st.sidebar.text_input("KR 기본 경로", value=DEFAULT_KR_PATH if os.path.exists(DEFAULT_KR_PATH) else "")
    us_repo = st.sidebar.text_input("US 기본 경로", value=DEFAULT_US_PATH if os.path.exists(DEFAULT_US_PATH) else "")
    eu_repo = st.sidebar.text_input("EU 기본 경로", value=DEFAULT_EU_PATH if os.path.exists(DEFAULT_EU_PATH) else "")

    # ----- 도우미: 업로드/경로 → 임시파일(또는 경로) 변환 -----
    def _to_path_or_buffer(uploaded_file, fallback_path: str):
        """
        - 업로드 파일이 있으면 그걸 메모리 버퍼로 반환
        - 아니면 fallback 경로가 실제로 존재하면 그 경로 사용
        - 둘 다 없으면 None
        """
        if uploaded_file is not None:
            data = uploaded_file.read()
            return io.BytesIO(data)
        if fallback_path and os.path.exists(fallback_path):
            return fallback_path
        return None

    kr_source = _to_path_or_buffer(kr_up, kr_repo)
    us_source = _to_path_or_buffer(us_up, us_repo)
    eu_source = _to_path_or_buffer(eu_up, eu_repo)

    # ----- 검색 옵션 -----
    st.sidebar.header("검색 옵션")
    algo_disp = {
        "token_set_ratio": "Token Set",
        "ratio": "Full Ratio",
        "partial_ratio": "Partial"
    }
    algo_key = st.sidebar.selectbox("알고리즘", list(algo_disp.keys()), format_func=lambda k: algo_disp[k], index=0)
    threshold = st.sidebar.slider("임계값", min_value=50, max_value=100, value=85, step=1)

    # ----- 상태 캐시 로딩 -----
    @st.cache_data(show_spinner=True)
    def _load_db(kind: str, source):
        db = ChemicalDB(kind, path="(in-memory)" if isinstance(source, io.BytesIO) else (source or ""))
        # in-memory인 경우 pandas로 직접 로딩해서 df 주입
        if isinstance(source, io.BytesIO):
            if "pandas" in _MISSING_DEPS:
                raise RuntimeError("pandas가 필요합니다.")
            df = pd.read_excel(source)
            df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]]
            db.df = df.fillna("")
            # kind별 초기화 로직 재사용 위해 columns 탐지 함수 호출 겸 load의 내부를 간단히 우회
            # -> load()는 파일 경로 체크를 하므로 여기서는 '간편 재설정'을 따로 수행
            if kind == "KR":
                db.cas_col          = find_col_fuzzy(db.df, ["cas no.", "cas number", "cas"])
                db.primary_name_col = find_col_fuzzy(db.df, ["영문명", "영문 제품명", "영문", "english name"])
                db.korean_name_col  = find_col_fuzzy(db.df, ["제품명", "품목명", "국문명", "한글명", "국문", "한글"])
                wanted = [db.cas_col, db.primary_name_col, db.korean_name_col]
                db.columns_to_display = [c for c in wanted if c is not None] or list(db.df.columns)
            elif kind == "US":
                db.cas_col          = find_col_fuzzy(db.df, ["cas reg. no.", "cas reg no", "cas no", "cas"])
                db.primary_name_col = find_col_fuzzy(db.df, ["substance"])
                wanted = [db.cas_col, db.primary_name_col]
                db.columns_to_display = [c for c in wanted if c is not None] or list(db.df.columns)
            elif kind == "EU":
                e_col    = find_col_fuzzy(db.df, ["e_number", "e number", "e-number"])
                name_col = find_col_fuzzy(db.df, ["additive_name_en", "additive name en", "name_en", "name en"])
                syn_col  = find_col_fuzzy(db.df, ["synonyms"])
                cas_list_col = find_col_fuzzy(db.df, ["cas_list", "cas list", "cas reg. no.", "cas no", "cas"])
                food_cat_col = find_col_fuzzy(db.df, ["food category", "food_category"])
                restr_col    = find_col_fuzzy(db.df, ["individual restriction(s) / exception(s)", "restrictions", "individual restrictions / exceptions"])
                foot_col     = find_col_fuzzy(db.df, ["footnotes", "footnote"])
                db.cas_col          = cas_list_col
                db.primary_name_col = name_col
                db.other_names_col  = syn_col
                preferred = [c for c in [e_col, name_col, syn_col, cas_list_col, food_cat_col, restr_col, foot_col] if c]
                rest = [c for c in db.df.columns if c not in preferred]
                db.columns_to_display = preferred + rest
            db.loaded = True
            return db
        else:
            # 파일 경로 모드 → 원래 load() 사용
            db.load()
            return db

    # ----- DB 준비 -----
    with st.spinner("데이터 준비 중…"):
        kr_db = _load_db("KR", kr_source) if kr_source else None
        us_db = _load_db("US", us_source) if us_source else None
        eu_db = _load_db("EU", eu_source) if eu_source else None

    # ----- 검색창 -----
    q = st.text_input("🔎 검색어(한글/영문명 또는 CAS No.)", key="query")
    col_k, col_u, col_e = st.columns(3)

    # ----- 검색 실행 -----
    def _search(db: ChemicalDB, query: str):
        if not db:
            return None
        try:
            return db.search(query, algo_key=algo_key, threshold=float(threshold))
        except Exception as e:
            st.warning(f"{db.kind} 검색 오류: {e}")
            return None

    if q:
        kr_res = _search(kr_db, q)
        us_res = _search(us_db, q)
        eu_res = _search(eu_db, q)

        # EU exact 중복 정리
        if eu_res:
            eu_res = SearchResult(
                exact_rows=App._eu_dedupe_exact(App, eu_res.exact_rows) if eu_res.exact_rows else [],
                similar_rows=eu_res.similar_rows
            )

        # 상태 배지
        def _badge(ok):
            return "✅ 사용 가능(정확일치)" if ok else "⚠️ 확인 필요(정확일치 없음)"

        st.success(
            f"KR: {_badge(bool(kr_res and kr_res.exact_rows))} | "
            f"US: {_badge(bool(us_res and us_res.exact_rows))} | "
            f"EU: {_badge(bool(eu_res and eu_res.exact_rows))}"
        )

        # 결과 그리드
        def _rows_to_df(db: ChemicalDB, rows: List[DBRow], include_score=False, scores=None):
            if not db or not rows:
                return pd.DataFrame()
            cols = db.columns_to_display
            out = []
            for i, r in enumerate(rows):
                row = {}
                if include_score:
                    row["유사도 점수"] = f"{scores[i]:.0f}" if scores else ""
                for c in cols:
                    val = r.data.get(c, "")
                    row[c] = "" if val is None else str(val)
                out.append(row)
            return pd.DataFrame(out)

        with col_k:
            st.subheader("🇰🇷 KR")
            if kr_res and (kr_res.exact_rows or kr_res.similar_rows):
                if kr_res.exact_rows:
                    st.markdown("**정확 일치**")
                    st.dataframe(_rows_to_df(kr_db, kr_res.exact_rows), use_container_width=True)
                if kr_res.similar_rows:
                    st.markdown("**유사 검색 결과**")
                    sim_rows = [r for s, r in kr_res.similar_rows]
                    sim_scores = [s for s, r in kr_res.similar_rows]
                    st.dataframe(_rows_to_df(kr_db, sim_rows, include_score=True, scores=sim_scores), use_container_width=True)
            else:
                st.info("결과 없음")

        with col_u:
            st.subheader("🇺🇸 US")
            if us_res and (us_res.exact_rows or us_res.similar_rows):
                if us_res.exact_rows:
                    st.markdown("**정확 일치**")
                    st.dataframe(_rows_to_df(us_db, us_res.exact_rows), use_container_width=True)
                if us_res.similar_rows:
                    st.markdown("**유사 검색 결과**")
                    sim_rows = [r for s, r in us_res.similar_rows]
                    sim_scores = [s for s, r in us_res.similar_rows]
                    st.dataframe(_rows_to_df(us_db, sim_rows, include_score=True, scores=sim_scores), use_container_width=True)
            else:
                st.info("결과 없음")

        with col_e:
            st.subheader("🇪🇺 EU")
            if eu_res and (eu_res.exact_rows or eu_res.similar_rows):
                if eu_res.exact_rows:
                    st.markdown("**정확 일치(그룹 중복 제거)**")
                    st.dataframe(_rows_to_df(eu_db, eu_res.exact_rows), use_container_width=True)
                if eu_res.similar_rows:
                    st.markdown("**유사 검색 결과**")
                    sim_rows = [r for s, r in eu_res.similar_rows]
                    sim_scores = [s for s, r in eu_res.similar_rows]
                    st.dataframe(_rows_to_df(eu_db, sim_rows, include_score=True, scores=sim_scores), use_container_width=True)
            else:
                st.info("결과 없음")

        st.write("---")

        # ----- 상세 보기(행 선택 → 디테일) -----
        st.subheader("🔎 상세 보기")
        st.caption("행을 선택해서 세부 내용을 보세요. (간단 모드)")

        # 간단 입력으로 행 index를 받아 상세 표시(실서비스에서는 AgGrid/DataEditor로 대체 가능)
        dcol1, dcol2, dcol3 = st.columns(3)
        src = dcol1.selectbox("DB", ["KR", "US", "EU"])
        which = dcol2.selectbox("타입", ["정확 일치", "유사 검색 결과"])
        idx = dcol3.number_input("행 인덱스(0부터)", min_value=0, value=0, step=1)

        def _pick(db, res):
            if not db or not res:
                return None
            if which == "정확 일치":
                rows = res.exact_rows
            else:
                rows = [r for s, r in res.similar_rows]
            if not rows or idx >= len(rows):
                return None
            return db, rows[idx]

        picked = None
        if src == "KR":
            picked = _pick(kr_db, kr_res)
        elif src == "US":
            picked = _pick(us_db, us_res)
        else:
            picked = _pick(eu_db, eu_res)

        if picked is None:
            st.info("유효한 행을 선택하세요.")
        else:
            db, dbrow = picked
            st.markdown(f"**[{db.kind}] 상세**")
            detail = {k: ("" if v is None else str(v)) for k, v in dbrow.data.items()}
            st.dataframe(pd.DataFrame([detail]).T, use_container_width=True)

            # 본문 링크에서 URL 추출 → 요약 버튼 제공
            all_text = " \n ".join(str(v) for v in detail.values())
            urls = extract_urls(all_text)
            urls = list(dict.fromkeys(urls))  # dedupe
            if urls:
                st.markdown("**문서/참조 링크**")
                for u in urls:
                    st.write(f"🔗 {u}")

                if st.button("AI로 주요 규정 요약하기 (링크 통합)", type="primary", disabled=not GEMINI_CONFIGURED):
                    with st.spinner("AI가 요약 중…"):
                        try:
                            headers = {'User-Agent': 'Mozilla/5.0'}
                            parts = []
                            for u in urls:
                                try:
                                    r = requests.get(u, timeout=15, headers=headers)
                                    r.raise_for_status()
                                    soup = BeautifulSoup(r.content, "html.parser")
                                    txt = soup.get_text(" ", strip=True)
                                    txt = re.sub(r"\s+", " ", txt)[:8000]
                                    parts.append(f"[SOURCE] {u}\n{txt}")
                                except Exception as e:
                                    parts.append(f"[SOURCE] {u}\n(수집 실패: {e})")
                            combined = "\n\n---\n\n".join(parts)[:120000]
                            if not GEMINI_CONFIGURED:
                                st.warning("Gemini 설정이 없어 요약을 진행할 수 없습니다.")
                            else:
                                with _silence_stderr_c():
                                    ans = GEMINI_MODEL.generate_content(
                                        "아래 원문 발췌를 근거로 한국어로 요약하되, 수치/용도/제한 중심으로 불릿 정리:\n\n" + combined
                                    ).text
                                st.markdown(ans or "_요약 결과 없음_")
                        except Exception as e:
                            st.error(f"요약 실패: {e}")
            else:
                st.caption("추출된 링크가 없습니다.")

    else:
        st.info("검색어를 입력하세요.")

# ---- 실행 분기: 환경변수나 실행 명령으로 웹/데스크톱 전환 ----
if __name__ == "__main__":
    # 우선순위: 1) 강제 웹 모드 2) Streamlit 런타임 감지 3) 기본(Tk)
    FORCE_WEB = os.getenv("RUN_WEB", "").strip() in ("1", "true", "TRUE")
    IN_STREAMLIT = any("streamlit" in (arg or "").lower() for arg in sys.argv)
    if FORCE_WEB or IN_STREAMLIT:
        run_streamlit()
    else:
        main()
