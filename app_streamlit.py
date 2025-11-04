# app_streamlit.py — Streamlit 웹 UI

import os, re
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from io import BytesIO

import semipro_core as core  # 위 코어 파일

# --- session init ---
if "last_results" not in st.session_state:
    st.session_state.last_results = None
if "query" not in st.session_state:
    st.session_state["query"] = ""   # ← 초기값만 세팅해두기

# ---- 페이지 설정 ----
st.set_page_config(page_title="SEMPIO Global Safety Research", layout="wide")

# ---- 샘표 CI 헤더 (CI와 타이틀 한 줄 정렬) ----
import base64

def _img_b64(path: str):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

_logo64 = _img_b64("sempio_logo.png")

# 여백/행간 최소화
st.markdown("""
<style>
h1, h3 { margin: 0 !important; }
.header-sub { margin-top: 0; color: #666; font-size:18px; }
[data-testid="stVerticalBlock"] div:has(> img) {
    align-items: center !important;
}
</style>
""", unsafe_allow_html=True)

col_logo, col_title = st.columns([1, 8], vertical_alignment="center")
with col_logo:
    if _logo64:
        st.image(f"data:image/png;base64,{_logo64}", width=120)
with col_title:
    st.markdown("## **Food Additives Database**", unsafe_allow_html=True)
    st.markdown('<div class="header-sub">국가별 식품첨가물 사용기준 검색</div>', unsafe_allow_html=True)

st.divider()


# --- 검색 입력/버튼 한 줄 정렬 ---
st.markdown("""
<style>
.stButton > button { height: 42px; }
</style>
""", unsafe_allow_html=True)

# ⬇️ 지우기 콜백 (여기서만 상태를 바꾸세요)
def _on_clear():
    st.session_state["query"] = ""              # 입력창 비우기
    st.session_state.pop("last_results", None)  # 결과 초기화
    st.session_state.pop("us_chat_history", None)
    st.session_state.pop("last_cfr_combined", None)
    # st.experimental_rerun()  # 콜백은 자동 재실행되므로 보통 불필요

# --- 검색 입력/버튼 한 줄 정렬 (정렬 개선) ---
st.markdown("""
<style>
div[data-testid="stHorizontalBlock"] {
    align-items: center !important;
}
.stTextInput>div>div>input {
    height: 44px !important;
}
.stButton>button {
    height: 44px !important;
    margin-top: 0 !important;
}
</style>
""", unsafe_allow_html=True)

# --- 검색 입력/버튼 한 줄 정렬 (수정 버전) ---
c1, c2, c3 = st.columns([6, 2, 2])
with c1:
    query = st.text_input(
        "원료명 또는 영문명 입력",
        key="query",
        placeholder="예) 글리신 / glycine / 56-40-6",
        label_visibility="visible",
    )

with c2:
    go = st.button("검색", use_container_width=True, type="primary")

        # ✅ 여기에 추가 — 검색 버튼 바로 아래!
    if query and st.session_state.get("query") == query:
        st.session_state["trigger_enter"] = True

with c3:
    st.button("지우기", use_container_width=True, on_click=_on_clear)

# ✅ 추가 — Enter키로 검색하도록 보조 로직
if st.session_state.get("trigger_enter", False) and not go:
    go = True
    st.session_state["trigger_enter"] = False  # 재입력 방지


# ---- Gemini (선택) ----
GEMINI_API_KEY = "AIzaSyDpPvneo1OyY2a6DUZHgSOWdpcbt9rVx4g"
GEMINI_MODEL = None
try:
    import google.generativeai as genai
    if GEMINI_API_KEY.strip():
        genai.configure(api_key=GEMINI_API_KEY.strip())
        GEMINI_MODEL = genai.GenerativeModel("gemini-2.5-flash")
except Exception as e:
    st.sidebar.warning(f"Gemini 로드 실패: {e}")


# ---- 사이드바: 파일 업로더 + 검색 설정 ----
with st.sidebar:
    st.header("데이터 파일 업로드")
    kr_file = st.file_uploader("KR 엑셀 업로드", type=["xlsx", "xls"], key="kr_file")
    us_file = st.file_uploader("US 엑셀 업로드", type=["xlsx", "xls"], key="us_file")
    eu_file = st.file_uploader("EU 엑셀 업로드", type=["xlsx", "xls"], key="eu_file")

    st.divider()
    algo = st.selectbox("유사도 알고리즘", ["token_set_ratio", "ratio", "partial_ratio"], index=0)
    thr  = st.slider("임계값", 50, 100, 85)

def _rewind(f):
    try:
        f.seek(0)
    except Exception:
        pass
    return f

def _filebytes(uploaded):
    _rewind(uploaded)
    return uploaded.read()

# 업로드 가드
if not (kr_file and us_file and eu_file):
    st.warning("왼쪽에서 KR/US/EU 엑셀을 모두 업로드하면 검색이 시작됩니다.")
    st.stop()

# ---- 검색 캐시 ----
@st.cache_data(show_spinner=False)
def search_records(kind: str, file_bytes: bytes, query: str, algo_key: str, threshold: float):
    key = f"{kind}_{hash(file_bytes)}_{algo_key}_{threshold}"
    bio = BytesIO(file_bytes)
    db = core.ChemicalDB(kind, bio)
    db.load()
    res = db.search(query, algo_key=algo_key, threshold=float(threshold))
    return db, res

# ---- US/EU 폴백 검색: best_term로 안 뜨면 확장어 순차 재시도 ----
def _search_with_fallback(kind: str, file_bytes: bytes, terms: list, algo_key: str, threshold: float):
    first_term = terms[0] if terms else ""
    db, res = search_records(kind, file_bytes, first_term, algo_key, float(threshold))
    if res.exact_rows:
        return db, res
    # exact이 하나도 없으면 확장어로 순차 재시도
    for t in terms[1:10]:
        _, r2 = search_records(kind, file_bytes, t, algo_key, float(threshold))
        if r2.exact_rows:
            return db, r2
        # exact이 없어도 similar이 훨씬 많으면 그걸 채택
        if (len(r2.similar_rows) > len(res.similar_rows)):
            res = r2
    return db, res

# ---- 확장어 중 "최적" 1개 고르기 ----
@st.cache_data(show_spinner=False)
def _choose_best_term(expanded_terms: list, kr_bytes: bytes, algo_key: str, threshold: float) -> str:
    """
    각 확장어에 대해 KR만 빠르게 스코어링하여 최고점을 선택.
    스코어 = (정확일치 수 * 2) + 유사일치 수
    """
    best_term, best_score = None, -1
    for t in expanded_terms[:10]:  # 과도한 반복 방지
        try:
            _, res = search_records("KR", kr_bytes, t, algo_key, float(threshold))
            score = (len(res.exact_rows) * 2) + len(res.similar_rows)
            if score > best_score:
                best_term, best_score = t, score
        except Exception as e:
            print(f"⚠️ term 스코어링 실패({t}): {e}")
            continue
    return best_term or (expanded_terms[0] if expanded_terms else "")


# --- (이 부분 위에는 캐시 함수 등 있을 수 있음) ---

def _expand_terms_korean(first_query: str, kr_bytes: bytes) -> list:
    """KR DB에서 영문명·CAS를 찾아 검색어 확장 (Gemini 미사용 버전)."""
    terms = [first_query]

    # 한글 포함 시 KR DB에서 영문명·CAS 추출
    if re.search(r"[가-힣]", first_query):
        try:
            kr_db = core.ChemicalDB("KR", BytesIO(kr_bytes))
            kr_db.load()
            extra_terms = kr_db.translate_korean_locally(first_query)
            if extra_terms:
                terms.extend(extra_terms)
        except Exception as e:
            print(f"⚠️ KR DB 확장 검색 실패: {e}")

    # 중복 제거 및 정리
    out, seen = [], set()
    for t in terms:
        if t and t not in seen:
            out.append(t)
            seen.add(t)
    return out

# --- 검색 실행(단일 블록) ---
if go:
    if not (kr_file and us_file and eu_file):
        st.warning("KR/US/EU 엑셀 파일을 모두 업로드해 주세요.")
        st.session_state.last_results = None
        st.stop()

    query_norm = (query or "").strip()
    if not query_norm:
        st.warning("검색어를 입력한 뒤 ‘검색’을 눌러주세요.")
        st.session_state.last_results = None
        st.stop()

    kr_bytes = _filebytes(kr_file)
    us_bytes = _filebytes(us_file)
    eu_bytes = _filebytes(eu_file)

    # 1) 확장어 생성 + KR 기준 최적어 선택
    expanded = _expand_terms_korean(query_norm, kr_bytes)
    best_term = _choose_best_term(expanded, kr_bytes, algo, float(thr))

    # 2) KR은 best_term로, US/EU는 exact 미발견 시 확장어 폴백
    with st.spinner(f"‘{best_term}’ 기준으로 KR/US/EU 검색 중..."):
        db_kr, res_kr = search_records("KR", kr_bytes, best_term, algo, float(thr))
        db_us, res_us = _search_with_fallback("US", us_bytes, [best_term] + expanded, algo, float(thr))
        db_eu, res_eu = _search_with_fallback("EU", eu_bytes, [best_term] + expanded, algo, float(thr))

    st.session_state.last_results = ((db_kr, res_kr), (db_us, res_us), (db_eu, res_eu))
    # 상태 요약 배지
    kr_cnt = len(res_kr.exact_rows)
    us_cnt = len(res_us.exact_rows)

    def _badge(label, ok):
        color = "green" if ok else "red"
        st.markdown(
            f"<span style='background:{color};color:white;padding:2px 8px;border-radius:8px'>{label}</span>",
            unsafe_allow_html=True
        )

    # ---- 국가별 사용 가능/확인 필요 배지 표시 ----

    cols_badge = st.columns(3)

    def _status_badge(country: str, count: int):
        """국가별 사용가능/확인필요 배지 표시"""
        ok = count > 0
        color = "#1b8300" if ok else "#b30000"  # 초록 / 빨강
        text = f"{country} 사용가능 ({count}건)" if ok else f"{country} 사용 확인필요 ({count}건)"
        st.markdown(
            f"""
            <div style='
                background:{color};
                color:white;
                padding:8px 12px;
                border-radius:10px;
                text-align:center;
                font-weight:bold;
                font-size:15px;
                box-shadow:0 1px 3px rgba(0,0,0,0.2);
            '>
                {text}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # 정확일치 건수 계산
    kr_cnt = len(res_kr.exact_rows)
    us_cnt = len(res_us.exact_rows)
    eu_cnt = len(res_eu.exact_rows)

    # 3개 컬럼에 배치
    with cols_badge[0]:
        _status_badge("한국", kr_cnt)
    with cols_badge[1]:
        _status_badge("미국", us_cnt)
    with cols_badge[2]:
        _status_badge("유럽", eu_cnt)

# --- 결과 보장 유틸: last_results 구조가 올바른지 검사 ---
def _valid_results(obj) -> bool:
    return (
        isinstance(obj, tuple) and len(obj) == 3 and
        all(isinstance(x, tuple) and len(x) == 2 for x in obj)
    )

# ← 언패킹하기 전에 반드시 가드
results = st.session_state.get("last_results", None)
if not _valid_results(results):
    st.info("왼쪽에서 KR/US/EU 파일을 업로드하고, 검색어 입력 → ‘검색’ 버튼을 눌러주세요.")
    st.stop()

(db_kr, res_kr), (db_us, res_us), (db_eu, res_eu) = results

if len(res_kr.exact_rows) == 0 and len(res_kr.similar_rows) == 0:
    st.info("검색 결과가 없습니다. 다른 이름 또는 CAS 번호로 시도해보세요.")

def _first_exact_title(db, res, fallback: str = ""):
    if not res.exact_rows:
        return fallback
    row = res.exact_rows[0].data
    name_col = None
    # 우선순위: KR=국문명/영문명, US=primary_name_col, EU=primary_name_col
    if db.kind == "KR":
        for c in [db.korean_name_col, db.primary_name_col]:
            if c and c in row and str(row.get(c, "")).strip():
                name_col = c; break
    else:
        name_col = db.primary_name_col
    title = str(row.get(name_col, "") or "").strip() if name_col else ""
    return title or fallback

tabs = st.tabs(["대한민국(KR)", "미국(US)", "유럽(EU)"])

def rows_to_df(db, rows, all_cols=False):
    cols = list(db.df.columns) if all_cols or not getattr(db, "columns_to_display", None) \
           else [c for c in db.columns_to_display if c]
    data = []
    for r in rows:
        data.append([r.data.get(c, "") for c in cols])
    return pd.DataFrame(data, columns=[str(c) for c in cols])

# =========================
# KR 탭
# =========================
with tabs[0]:
    st.subheader(f"정확히 일치 – {_first_exact_title(db_kr, res_kr, '없음')}")

        # ✅ 모든 열 보기 체크박스 제거
    df_exact = rows_to_df(db_kr, res_kr.exact_rows, all_cols=True)
    df_similar = rows_to_df(db_kr, [r for _, r in res_kr.similar_rows], all_cols=True)


    st.markdown("**정확히 일치한 결과**")
    selected_row = st.data_editor(
        df_exact,
        hide_index=True,
        use_container_width=True,
        key="selected_exact_row",
        disabled=True,
    )

    st.markdown("**유사 검색 결과**")
    st.data_editor(
        df_similar,
        hide_index=True,
        use_container_width=True,
        key="selected_similar_row",
        disabled=True,
    )

    # ✅ 클릭된 행의 상세정보 표시
    if selected_row is not None and len(df_exact) > 0:
        st.divider()
        st.markdown("### 상세보기")

        # ✅ 안전하게 인덱스 추출 (신규 Streamlit 호환)
        try:
            clicked_idx = list(selected_row.index)[0]
        except Exception:
            clicked_idx = None

        if clicked_idx is not None and clicked_idx < len(res_kr.exact_rows):
            row_data = res_kr.exact_rows[clicked_idx].data

            html = ["<table style='width:100%;border-collapse:collapse;'>"]
            for k, v in row_data.items():
                vv = str(v or "")
                html.append(
                    f"<tr>"
                    f"<td style='width:22%;padding:6px 8px;background:#f9f9f9;border:1px solid #ddd;'><b>{k}</b></td>"
                    f"<td style='padding:6px 8px;border:1px solid #ddd;'>{vv}</td>"
                    f"</tr>"
                )
            html.append("</table>")
            st.markdown("\n".join(html), unsafe_allow_html=True)


# =========================
# US 탭
# =========================
def _is_cfr(u: str) -> bool:
    u = (u or "").lower()
    return ("ecfr.gov" in u) or ("govinfo.gov" in u) or ("law.cornell.edu/cfr" in u) or ("/cfr/" in u)

def _fetch_with_fallback(url: str, timeout=7):
    headers = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari'}
    try:
        r = requests.get(url, timeout=timeout, headers=headers)
        if r.status_code == 200 and r.content:
            return r.content.decode(errors="ignore")
    except Exception:
        pass
    try:
        proxy = f"https://r.jina.ai/http://{url.replace('https://','').replace('http://','')}"
        r = requests.get(proxy, timeout=timeout, headers=headers)
        if r.status_code == 200 and r.text:
            return r.text
    except Exception:
        pass
    return ""


def _extract_cfr_text_fast(urls, timeout=5, max_workers=8, stop_after=5):
    headers = {'User-Agent':'Mozilla/5.0'}
    urls = list(dict.fromkeys([u for u in urls if _is_cfr(u)]))
    if not urls:
        return ""

    def fetch_one(u: str):
        try:
            raw = _fetch_with_fallback(u, timeout=timeout)
            if not raw:
                return f"[SOURCE] {u}\n(수집 실패: 차단/네트워크)", False
            soup = BeautifulSoup(raw, "html.parser")

            for sel in ["nav","header","footer","script","style","aside"]:
                for el in soup.select(sel):
                    el.decompose()
            text = soup.get_text(" ", strip=True)
            text = re.sub(r"\s+", " ", text)
            keep = []
            for ln in re.split(r"[;\.\n]\s+", text):
                if re.search(r"\b(ppm|mg/kg|mg/L|%|percent|GMP|quantum\s+satis|not\s+more\s+than|"
                             r"shall\s+not\s+exceed|max|limit|residue|used\s+as|for\s+use\s+as)\b", ln, re.I):
                    keep.append(ln.strip())
            text2 = "\n".join(dict.fromkeys(keep)) or text[:3000]
            return f"[SOURCE] {u}\n{text2}", True
        except Exception as e:
            return f"[SOURCE] {u}\n(수집 실패: {e})", False

    parts, ok_cnt = [], 0
    with ThreadPoolExecutor(max_workers=min(max_workers, len(urls))) as ex:
        futs = [ex.submit(fetch_one, u) for u in urls]
        for fut in as_completed(futs):
            txt, ok = fut.result()
            parts.append(txt); ok_cnt += int(ok)
            if ok_cnt >= stop_after:
                for f in futs:
                    if not f.done(): f.cancel()
                break
    return "\n\n---\n\n".join(parts)

def gemini_summarize_cfr(combined_source: str) -> str:
    if not GEMINI_MODEL:
        return "(Gemini 비활성)"
    prompt = (
        "아래는 CFR(연방규정) 섹션들에서 추출·정리한 본문 발췌입니다. "
        "탐색/검색 안내문 등 비규제성 문구는 무시하고, 다음 항목만 한국어로 간결하게 정리하세요.\n"
        "1) 섹션 번호/제목, 2) 용도(Used as/For), 3) 허용 한도(수치: %, mg/kg, mg/L, ppm, GMP/quantum satis 등), "
        "4) 제한/예외, 5) 주의·비고. 수치가 명시된 문장만 우선합니다.\n"
        "- 동일/유사 규정은 병합하고, 상충 시 둘 다 표기하며 각 항목 끝에 (출처: URL) 붙이세요.\n"
        "- 결과는 표 형태 없이 불릿 리스트로 번호를 매겨 주세요.\n\n"
        f"{combined_source[:80000]}"
    )
    return GEMINI_MODEL.generate_content(prompt).text

with tabs[1]:
    st.subheader(f"정확히 일치 – {_first_exact_title(db_us, res_us, '없음')}")
    st.dataframe(rows_to_df(db_us, res_us.exact_rows, all_cols=True))
    st.subheader("유사 검색 결과")
    st.dataframe(rows_to_df(db_us, [r for _, r in res_us.similar_rows], all_cols=True), use_container_width=True)

    st.divider()
    st.subheader("상세보기 + CFR 개별 요약")

    tgt = res_us.exact_rows[0] if res_us.exact_rows else (res_us.similar_rows[0][1] if res_us.similar_rows else None)
    if tgt:
        row = tgt.data
        # 모든 셀에서 URL 추출 후 CFR만
        urls = []
        for v in row.values():
            urls += core.extract_urls(str(v))
        cfr_urls = [u for u in urls if _is_cfr(u)]
        st.write("찾은 CFR 링크:", cfr_urls or "(없음)")

        # URL별 개별 요약
        for i, u in enumerate(cfr_urls):
            cols = st.columns([3, 1, 6])  # ✅ 링크 왼쪽, 버튼 오른쪽, 결과 중앙
            with cols[0]:
                st.code(u, language="text")
            with cols[1]:
                if st.button("요약", key=f"us_sum_{i}"):
                    with st.spinner("원문 수집/요약 중..."):
                        combined = _extract_cfr_text_fast([u], timeout=5, max_workers=1, stop_after=1)
                        summary = gemini_summarize_cfr(combined) if combined else "(수집 실패)"
                    st.session_state[f"us_sum_out_{i}"] = summary
            with cols[2]:
                if f"us_sum_out_{i}" in st.session_state:
                    st.text_area("요약 결과", st.session_state[f"us_sum_out_{i}"], height=500, key=f"us_sum_display_{i}")


        # --- 미국 상세 Q&A (채팅 컨텍스트 보기 제거) ---
        st.markdown("### Gemini AI Q&A (미국 상세)")
        base_ctx = "\n".join([f"{k}: {v}" for k, v in row.items()])[:120000]

        if "us_chat_history" not in st.session_state:
            st.session_state["us_chat_history"] = []

        for turn in st.session_state["us_chat_history"]:
            st.markdown(f"**Q:** {turn['q']}")
            st.markdown(f"**A:** {turn['a']}")
            st.markdown("---")

        with st.form("us_chat_form", clear_on_submit=True):
            q_us = st.text_input("질문을 입력하세요", "")
            submitted = st.form_submit_button("질문하기")
            if submitted and q_us.strip():
                if GEMINI_MODEL:
                    prompt = (
                        "다음 컨텍스트를 근거로 간단명료하게 한국어로 답해주세요.\n\n"
                        f"{base_ctx}\n\n질문: {q_us.strip()}"
                    )
                    answer = GEMINI_MODEL.generate_content(prompt).text
                else:
                    answer = "(Gemini 비활성)"
                st.session_state["us_chat_history"].append({"q": q_us.strip(), "a": answer})
                st.rerun()
        with st.expander("행 전체 세부정보 펼치기 (모든 열)"):
            st.markdown("**선택 행 전체 필드**")

            def _render_row_details(row_dict: dict):
                html = ["<table class='kv-table' style='width:100%;border-collapse:collapse'>"]
                for k, v in row_dict.items():
                    vv = str(v or "")
                    html.append(
                        f"<tr><td style='width:22%;vertical-align:top;padding:4px 8px;background:#f6f6f6'><b>{k}</b></td>"
                        f"<td style='padding:4px 8px'>{vv}</td></tr>"
                    )
                html.append("</table>")
                st.markdown("\n".join(html), unsafe_allow_html=True)

            _render_row_details(tgt.data)

# =========================
# EU 탭
# =========================
with tabs[2]:
    st.subheader(f"정확히 일치 – {_first_exact_title(db_eu, res_eu, '없음')}")

    # 1) 이름만 리스트업
    name_col = db_eu.primary_name_col or "additive_name_en"
    def _df_name_only(db, res):
        rows = res.exact_rows
        if not rows:
            rows = [r for _, r in res.similar_rows]
        data = []
        for r in rows:
            row = r.data
            nm = str(row.get(name_col, "") or "").strip()
            if not nm:
                # name_col이 없거나 비어있으면 첫 컬럼 사용
                nm = str(row.get(db.columns_to_display[0], "") or "")
            data.append({"name": nm})
        # 중복 제거
        seen, out = set(), []
        for d in data:
            if d["name"] and d["name"] not in seen:
                out.append(d); seen.add(d["name"])
        return pd.DataFrame(out, columns=["name"])

    df_names = _df_name_only(db_eu, res_eu)
    st.dataframe(df_names, use_container_width=True, height=min(400, 38*(len(df_names)+1)))

    # 2) 선택 → 상세보기
    target_name = st.selectbox("상세보기 대상(첨가물 명) 선택", options=df_names["name"].tolist() if not df_names.empty else [])
    if target_name:
        # 같은 이름의 모든 페이지(행) 모아 상세 표 표시
        def _filter_rows_all_pages(db, res, name):
            rows_all = []
            for r in res.exact_rows:
                if str(r.data.get(name_col, "")).strip() == name:
                    rows_all.append(r)
            for _, r in res.similar_rows:
                if str(r.data.get(name_col, "")).strip() == name:
                    rows_all.append(r)
            return rows_all

        all_rows = _filter_rows_all_pages(db_eu, res_eu, target_name)
        st.subheader(f"상세보기 – {target_name} (총 {len(all_rows)} 페이지)")
        st.dataframe(rows_to_df(db_eu, all_rows, all_cols=True))
        # (1) EU 특수 열 요약 표
        used_for_col = getattr(db_eu, "eu_used_for_col", None)
        ml_notes_col = getattr(db_eu, "eu_ml_food_notes_col", None)

        if all_rows:
            row0 = all_rows[0].data
            uf_val = str(row0.get(used_for_col, "") or "") if used_for_col else ""
            ml_val = str(row0.get(ml_notes_col, "") or "") if ml_notes_col else ""

            if used_for_col or ml_notes_col:
                st.markdown("**Used For & ML / Food notes 요약 표**")
                df_eu_special = pd.DataFrame(
                    [{"구분": "Used For", "내용": uf_val},
                    {"구분": "ML & Food notes", "내용": ml_val}]
                )
                st.dataframe(df_eu_special, use_container_width=True, height=140)

