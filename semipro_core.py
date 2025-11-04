# semipro_core.py  — Streamlit용 코어(순수 로직만)

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple, Union
import re, io
import pandas as pd

ROW_ID_COL = "__ROW_ID__"
CAS_PATTERN = re.compile(r"\b\d{2,7}-\d{2}-\d\b")

# (선택) rapidfuzz 사용
_USE_RAPIDFUZZ = False
try:
    from rapidfuzz import fuzz
    _USE_RAPIDFUZZ = True
except Exception:
    from difflib import SequenceMatcher


def extract_urls(text: str) -> List[str]:
    if not isinstance(text, str):
        return []
    url_re = re.compile(r"(https?://[^\s,{}()]+)", re.IGNORECASE)
    return url_re.findall(text)


def read_excel_flexible(
    src: Union[str, bytes, bytearray, io.BytesIO, pd.DataFrame, object]
) -> pd.DataFrame:
    """
    Streamlit UploadedFile / 파일경로(str) / bytes / BytesIO / DataFrame 모두 허용.
    - 확장자/콘텐츠타입 기반으로 엔진 지정
    - CSV 자동 처리
    """
    import io, os
    import pandas as pd

    if isinstance(src, pd.DataFrame):
        return src.copy()
    if src is None:
        raise ValueError("No source provided")

    # UploadedFile 같은 객체 속성 추출
    name = getattr(src, "name", None)
    mime = getattr(src, "type", None)

    # 바이트 확보 (getvalue 우선, 없으면 read)
    data_bytes = None
    if isinstance(src, (bytes, bytearray)):
        data_bytes = bytes(src)
    elif isinstance(src, io.BytesIO):
        data_bytes = src.getvalue()
    elif hasattr(src, "getvalue"):
        try:
            data_bytes = src.getvalue()
        except Exception:
            pass
    if data_bytes is None and hasattr(src, "read"):
        data_bytes = src.read()

    def _as_buffer():
        return io.BytesIO(data_bytes) if data_bytes is not None else src

    # CSV 판단
    def _is_csv(nm, mt):
        nm = (nm or "").lower()
        mt = (mt or "").lower()
        return nm.endswith(".csv") or "text/csv" in mt or "application/csv" in mt

    if _is_csv(name, mime):
        return pd.read_csv(_as_buffer())

    # 확장자로 엔진 선택
    ext = (os.path.splitext(name)[1].lower() if name else "").strip()

    def _read_with_engine(engine: str):
        return pd.read_excel(_as_buffer(), engine=engine)

    # 1) 확장자 기반 엔진 선택
    if ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        try:
            return _read_with_engine("openpyxl")
        except Exception:
            pass
    elif ext == ".xls":
        try:
            return _read_with_engine("xlrd")
        except Exception:
            pass
    elif ext == ".xlsb":
        try:
            return _read_with_engine("pyxlsb")
        except Exception:
            pass

    # 2) 확장자 불명/실패 시 폴백
    last_err = None
    for eng in ("openpyxl", "xlrd", "pyxlsb"):
        try:
            return _read_with_engine(eng)
        except Exception as e:
            last_err = e
            continue

    # 3) 최종 폴백
    try:
        return pd.read_excel(_as_buffer())
    except Exception as e:
        raise ValueError(
            "엑셀 파일을 읽지 못했습니다. 파일 포맷/엔진을 확인하세요. "
            "권장 엔진: openpyxl(xlsx), xlrd==1.2.0(xls), pyxlsb(xlsb). "
            f"마지막 오류: {last_err or e}"
        )



def find_col_fuzzy(df: "pd.DataFrame", names: List[str]) -> Optional[str]:
    low_map = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        key = str(n).strip().lower()
        if key in low_map:
            return low_map[key]
    for n in names:
        key = str(n).strip().lower()
        for k, orig in low_map.items():
            if key in k:
                return orig
    return None


def _normalize(s: Any) -> str:
    return "" if s is None else str(s).strip()


def score_pair(query: str, candidate: str, algo_key: str) -> float:
    q = _normalize(query); c = _normalize(candidate)
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
        return 100.0 * SequenceMatcher(None, q.lower(), c.lower()).ratio()


@dataclass
class DBRow:
    idx: int
    data: Dict[str, Any]


@dataclass
class SearchResult:
    exact_rows: List[DBRow]
    similar_rows: List[Tuple[float, DBRow]]


class ChemicalDB:
    def __init__(self, kind: str, source):
        """
        source: Streamlit UploadedFile, 파일경로(str), bytes, DataFrame 모두 지원
        """
        self.kind = kind
        self.source = source
        self.df: Optional[pd.DataFrame] = None
        self.columns_to_display: List[str] = []
        self.cas_col = None
        self.primary_name_col = None
        self.other_names_col = None
        self.korean_name_col = None
        self.loaded = False
        self.eu_used_for_col = None
        self.eu_ml_food_notes_col = None

    def load(self):
        df = read_excel_flexible(self.source)
        # Unnamed 제거 + 결측 치환
        df = df.loc[:, [c for c in df.columns if not str(c).startswith("Unnamed")]].fillna("")

        if self.kind == "KR":
            self.cas_col          = find_col_fuzzy(df, ["cas no.", "cas number", "cas"])
            self.primary_name_col = find_col_fuzzy(df, ["영문명", "영문 제품명", "영문", "english name"])
            self.korean_name_col  = find_col_fuzzy(df, ["제품명", "품목명", "국문명", "한글명", "국문", "한글"])
            wanted = [self.cas_col, self.primary_name_col, self.korean_name_col]
            self.columns_to_display = [c for c in wanted if c] or list(df.columns)

        elif self.kind == "US":
            self.cas_col          = find_col_fuzzy(df, ["cas reg. no.", "cas reg no", "cas no", "cas"])
            self.primary_name_col = find_col_fuzzy(df, ["substance"])
            wanted = [self.cas_col, self.primary_name_col]
            self.columns_to_display = [c for c in wanted if c] or list(df.columns)

        elif self.kind == "EU":
            e_col    = find_col_fuzzy(df, ["e_number", "e number", "e-number"])
            name_col = find_col_fuzzy(df, ["additive_name_en", "additive name en", "name_en", "name en"])
            syn_col  = find_col_fuzzy(df, ["synonyms"])
            cas_list_col = find_col_fuzzy(df, ["cas_list", "cas list", "cas reg. no.", "cas no", "cas"])
            food_cat_col = find_col_fuzzy(df, ["food category", "food_category"])
            restr_col    = find_col_fuzzy(df, ["individual restriction(s) / exception(s)", "restrictions", "individual restrictions / exceptions"])
            foot_col     = find_col_fuzzy(df, ["footnotes", "footnote"])

            self.eu_used_for_col = find_col_fuzzy(df, [
                "used for", "used_for", "use for", "use(s) for", "uses", "usedfor"
            ])
            self.eu_ml_food_notes_col = find_col_fuzzy(df, [
                "ml & food notes", "ml and food notes", "ml / food notes",
                "ml_food_notes", "ml", "food notes", "ml/food notes"
            ])

            self.cas_col          = cas_list_col
            self.primary_name_col = name_col
            self.other_names_col  = syn_col

            preferred = [c for c in [e_col, name_col, syn_col, cas_list_col, food_cat_col, restr_col, foot_col] if c]
            rest = [c for c in df.columns if c not in preferred]
            self.columns_to_display = preferred + rest

        else:
            self.columns_to_display = list(df.columns)

        self.df = df
        self.loaded = True

    def _row_candidates(self, row: "pd.Series") -> List[str]:
        cand = []
        if self.cas_col:
            text = str(row.get(self.cas_col, "") or "")
            parts = re.findall(CAS_PATTERN, text) or ([text.strip()] if text.strip() else [])
            cand.extend(parts)
        if self.primary_name_col:
            cand.append(_normalize(row.get(self.primary_name_col, "")))
        if self.kind == "KR" and self.korean_name_col:
            cand.append(_normalize(row.get(self.korean_name_col, "")))
        if self.kind in ("US", "EU") and self.other_names_col:
            other = str(row.get(self.other_names_col, "") or "")
            cand.extend([p.strip() for p in re.split(r";", other) if p.strip()])
        return [x for x in cand if x]

    def search(self, query: str, algo_key: str, threshold: float, max_similar: int = 50) -> SearchResult:
        if not self.loaded:
            self.load()
        q = _normalize(query)
        m = CAS_PATTERN.search(q)
        if m:
            q = m.group(0)
        ql = q.lower()

        exact_rows: List[DBRow] = []
        similar_rows: List[Tuple[float, DBRow]] = []

        for idx, row in self.df.iterrows():
            cands = self._row_candidates(row)
            is_exact = any(c.lower() == ql for c in cands)
            contains = any(ql in c.lower() for c in cands if c)

            if is_exact:
                exact_rows.append(DBRow(idx=idx, data=row.to_dict()))
                continue

            best = 0.0
            for c in cands:
                s = score_pair(q, c, algo_key)
                if s > best:
                    best = s
                if contains:
                    best = max(best, 99.0)
            if best >= threshold:
                similar_rows.append((best, DBRow(idx=idx, data=row.to_dict())))

        similar_rows.sort(key=lambda x: (-x[0], x[1].idx))
        if len(similar_rows) > max_similar:
            similar_rows = similar_rows[:max_similar]
        return SearchResult(exact_rows=exact_rows, similar_rows=similar_rows)
    def translate_korean_locally(self, korean_query: str) -> List[str]:
        if not self.loaded or self.df is None or not self.korean_name_col:
            return []
        q = _normalize(korean_query).lower()
        match = self.df[self.df[self.korean_name_col].str.lower() == q]
        terms = set()
        if not match.empty:
            r = match.iloc[0]
            if self.primary_name_col:
                terms.add(_normalize(r.get(self.primary_name_col, "")))
            if self.cas_col:
                cas = _normalize(r.get(self.cas_col, ""))
                m = CAS_PATTERN.search(cas)
                if m: terms.add(m.group(0))
        return [t for t in terms if t]


def build_db(kind: str, src):
    db = ChemicalDB(kind, src)
    db.load()
    return db

def prepare_databases(kr_src, us_src, eu_src):
    kr_db = build_db("KR", kr_src)
    us_db = build_db("US", us_src)
    eu_db = build_db("EU", eu_src)
    return kr_db, us_db, eu_db
