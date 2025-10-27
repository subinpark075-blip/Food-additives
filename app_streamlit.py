# app_streamlit.py — Streamlit 웹 UI

import os, re
from concurrent.futures import ThreadPoolExecutor, as_completed

import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup

import semipro_core as core  # 위 코어 파일

# --- session init ---
if "last_results" not in st.session_state:
    st.session_state.last_results = None

# ---- 페이지 설정 ----
st.set_page_config(page_title="SEMPIO Global Safety Research", layout="wide")

# ---- 샘표 CI 헤더 ----
import base64, io

def _img_b64(path: str):
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

_logo64 = _img_b64("sempio_logo.png")
st.markdown(
    f"""
    <div style="display:flex;align-items:center;margin:12px 0 12px 0;">
      {f'<img src="data:image/png;base64,{_logo64}" style="width:54px;margin-right:14px;">' if _logo64 else ''}
      <div style="line-height:1.25">
        <h1 style="margin:0;font-weight:800;">SEMPIO Global Safety Research</h1>
        <h3 style="margin:4px 0 0 0;color:gray;">Food Additives Database</h3>
        <div style="margin:4px 0 0 0;color:#666;">국가별 식품첨가물 사용기준 검색</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True
)
st.write("---")
# --- 본문 상단 검색창(스크린샷 위치) ---
c1, c2, c3 = st.columns([5, 2, 1])
with c1:
    query = st.text_input("원료명 또는 영문명 입력", placeholder="예) 글리신 / glycine / 56-40-6")
with c2:
    st.write("&nbsp;")
    go = st.button("검색", type="primary")
with c3:
    st.write("&nbsp;")
    clear = st.button("지우기")
if clear:
    st.session_state.pop("last_results", None)

# ---- Gemini (선택) ----
GEMINI_API_KEY = "여기에_본인_Gemini_API_키"
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

# 업로드 가드
if not (kr_file and us_file and eu_file):
    st.warning("왼쪽에서 KR/US/EU 엑셀을 모두 업로드하면 검색이 시작됩니다.")
    st.stop()

# ---- 검색 캐시 ----
@st.cache_data(show_spinner=False)
def search_records(kind: str, fileobj, query: str, algo_key: str, threshold: float):
    db = core.ChemicalDB(kind, fileobj)
    db.load()
    res = db.search(query, algo_key=algo_key, threshold=float(threshold))
    return db, res

# --- (이 부분 위에는 캐시 함수 등 있을 수 있음) ---

def _expand_terms_korean(first_query: str) -> list:
    """KR DB에서 영문명·CAS를 찾아 검색어 확장 (Gemini 미사용 버전)."""
    terms = [first_query]

    # 한글 포함 시 KR DB에서 영문명·CAS 추출
    if re.search(r"[가-힣]", first_query):
        try:
            kr_db = core.ChemicalDB("KR", df_kr)
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


# --- 검색 실행 ---
if go:
    q = (query or "").strip()
    if not q:
        st.warning("검색어를 입력하세요.")
        st.stop()

    expanded_terms = _expand_terms_korean(q)

    with st.spinner("검색 중..."):
        res_map = _search_all(expanded_terms, algo, float(thr))

    (db_kr, res_kr) = res_map["KR"]
    (db_us, res_us) = res_map["US"]
    (db_eu, res_eu) = res_map["EU"]

    total_found = len(res_kr.exact_rows) + len(res_us.exact_rows) + len(res_eu.exact_rows)
    st.success(f"🔎 검색 결과: {total_found}건이 검색되었습니다.")

    st.session_state.last_results = ((db_kr, res_kr), (db_us, res_us), (db_eu, res_eu))


# --- 검색 실행 ---
if go:
    # 1) 파일 확인 (필요하다면)
    if not (kr_file and us_file and eu_file):
        st.warning("KR/US/EU 엑셀 파일을 모두 업로드해 주세요.")
        st.session_state.last_results = None
        st.stop()

    # 2) 검색어 확인
    query_norm = (query or "").strip()
    if not query_norm:
        st.warning("검색어를 입력한 뒤 ‘검색’을 눌러주세요.")
        st.session_state.last_results = None
        st.stop()

    # 3) 실제 검색
    with st.spinner("검색 중..."):
        db_kr, res_kr = search_records("KR", kr_file, query_norm, algo, float(thr))
        db_us, res_us = search_records("US", us_file, query_norm, algo, float(thr))
        db_eu, res_eu = search_records("EU", eu_file, query_norm, algo, float(thr))

    # 4) 성공 시에만 저장
    st.session_state.last_results = ((db_kr, res_kr), (db_us, res_us), (db_eu, res_eu))

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

tabs = st.tabs(["대한민국(KR)", "미국(US)", "유럽(EU)"])

def rows_to_df(db, rows):
    cols = [c for c in db.columns_to_display if c]
    data = []
    for r in rows:
        row = [r.data.get(c, "") for c in cols]
        data.append(row)
    return pd.DataFrame(data, columns=[str(c) for c in cols])

# =========================
# KR 탭
# =========================
with tabs[0]:
    st.subheader("정확히 일치")
    st.dataframe(rows_to_df(db_kr, res_kr.exact_rows), use_container_width=True)
    st.subheader("유사 검색 결과")
    st.dataframe(rows_to_df(db_kr, [r for _, r in res_kr.similar_rows]), use_container_width=True)

    st.divider()
    st.subheader("상세보기")
    target = res_kr.exact_rows[0] if res_kr.exact_rows else (res_kr.similar_rows[0][1] if res_kr.similar_rows else None)
    if target:
        row = target.data

        # 사용기준 원문 추출
        usage_keys = ["사용기준","사용 기준","사용기준(국내기준)","사용기준(국내기준)_업데이트"]
        usage = ""
        for k in usage_keys:
            if k in row:
                usage = str(row.get(k, "") or ""); break

        st.markdown("**사용기준 – AI 표 요약**")
        if GEMINI_MODEL and usage.strip():
            with st.spinner("AI가 표 형태로 정리 중..."):
                prompt = (
                    "다음 '사용기준' 한국어 원문을 표로 구조화해줘.\n"
                    "- 컬럼은 가능하면 다음 순서로: 식품유형\t허용기준(수치)\t근거/조건\t비고\n"
                    "- 수치는 mg/kg, mg/L, %, 또는 GMP/quantum satis 등 명확표기.\n"
                    "- 반드시 TSV(탭 구분)만 출력.\n\n"
                    f"{usage[:6000]}"
                )
                tsv = GEMINI_MODEL.generate_content(prompt).text.strip()

            lines = [ln for ln in tsv.splitlines() if ln.strip()]
            if len(lines) >= 2:
                headers = [h.strip() for h in lines[0].split("\t")]
                rows = [[c.strip() for c in ln.split("\t")] for ln in lines[1:]]
                df_tsv = pd.DataFrame(rows, columns=headers)
                # 초기 화면에서도 텍스트가 잘리더라도 모든 셀 N줄(예: 4줄)까지 표시될 수 있도록 높이 확보
                st.dataframe(df_tsv, use_container_width=True, height=min(600, 120 + 24*max(4, len(rows))))
            else:
                st.info("TSV 파싱에 실패했습니다.")
        else:
            st.info("API 키가 없거나 사용기준 원문이 없습니다.")

        st.markdown("**AI 질문**")
        q_kr = st.text_input("질문 입력", key="kr_q")
        if st.button("질문하기", key="kr_q_btn") and GEMINI_MODEL and q_kr.strip():
            ctx = "다음 텍스트만 근거로 간단히 한국어로 답하세요.\n\n" + (usage or "")
            with st.spinner("답변 생성 중..."):
                ans = GEMINI_MODEL.generate_content(f"{ctx}\n\n질문: {q_kr.strip()}").text
            st.write(ans)

# =========================
# US 탭
# =========================
def _is_cfr(u: str) -> bool:
    u = (u or "").lower()
    return ("ecfr.gov" in u) or ("govinfo.gov" in u) or ("law.cornell.edu/cfr" in u) or ("/cfr/" in u)

def _extract_cfr_text_fast(urls, timeout=5, max_workers=8, stop_after=5):
    headers = {'User-Agent':'Mozilla/5.0'}
    urls = list(dict.fromkeys([u for u in urls if _is_cfr(u)]))
    if not urls:
        return ""

    def fetch_one(u: str):
        try:
            r = requests.get(u, timeout=timeout, headers=headers)
            r.raise_for_status()
            soup = BeautifulSoup(r.content, "html.parser")
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
        f"{combined_source[:120000]}"
    )
    return GEMINI_MODEL.generate_content(prompt).text

with tabs[1]:
    st.subheader("정확히 일치")
    st.dataframe(rows_to_df(db_us, res_us.exact_rows), use_container_width=True)
    st.subheader("유사 검색 결과")
    st.dataframe(rows_to_df(db_us, [r for _, r in res_us.similar_rows]), use_container_width=True)

    st.divider()
    st.subheader("상세보기 + CFR 통합 요약")
    tgt = res_us.exact_rows[0] if res_us.exact_rows else (res_us.similar_rows[0][1] if res_us.similar_rows else None)
    if tgt:
        row = tgt.data
        # 모든 셀에서 URL 추출 후 CFR만
        urls = []
        for v in row.values():
            urls += core.extract_urls(str(v))
        cfr_urls = [u for u in urls if _is_cfr(u)]
        st.write("찾은 CFR 링크:", cfr_urls or "(없음)")

        if st.button(f"관련 원문 AI 요약 (최대 {min(len(cfr_urls), 8)}개)"):
            with st.spinner("원문 수집/요약 중..."):
                combined = _extract_cfr_text_fast(cfr_urls[:8], timeout=5, max_workers=8, stop_after=5)
                summary = gemini_summarize_cfr(combined) if combined else "(수집 실패)"
            st.session_state["last_cfr_combined"] = combined or ""
            st.text_area("요약", summary, height=300)

        # --- 미국 상세 채팅 ---
        st.markdown("### Gemini AI 채팅 (미국 상세)")
        base_ctx = st.session_state.get("last_cfr_combined", "")
        if not base_ctx:
            # 버튼을 안 눌렀을 때는 상세행 전체를 컨텍스트로 사용
            try:
                base_ctx = "\n".join([f"{k}: {v}" for k, v in row.items()])
            except Exception:
                base_ctx = "(컨텍스트 없음)"

        with st.expander("현재 채팅 컨텍스트 보기", expanded=False):
            st.text_area("컨텍스트", base_ctx[:120000], height=240)

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
                        f"{base_ctx[:120000]}\n\n질문: {q_us.strip()}"
                    )
                    answer = GEMINI_MODEL.generate_content(prompt).text
                else:
                    answer = "(Gemini 비활성)"
                st.session_state["us_chat_history"].append({"q": q_us.strip(), "a": answer})
                st.experimental_rerun()

# =========================
# EU 탭
# =========================
with tabs[2]:
    st.subheader("정확히 일치")
    st.dataframe(rows_to_df(db_eu, res_eu.exact_rows), use_container_width=True)
    st.subheader("유사 검색 결과")
    st.dataframe(rows_to_df(db_eu, [r for _, r in res_eu.similar_rows]), use_container_width=True)

    st.info("EU 그룹 페이지 URL이 있으면 별도 파서로 확장 가능(추후).")
