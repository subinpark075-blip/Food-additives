#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import base64
import pandas as pd
import streamlit as st
import google.generativeai as genai
from threading import Thread

# ---------------------------
# 페이지 & 전역 스타일
# ---------------------------
st.set_page_config(page_title="SEMPIO Global Gate - Food Additives", layout="wide")

CUSTOM_CSS = """
<style>
/* 상단 여백/폰트 */
h1, h2, h3 { letter-spacing: .2px; }
.top-wrap { display:flex; align-items:center; margin: 10px 0 20px 0; }
.top-wrap img { width:60px; margin-right:15px; margin-top:-3px; }
.top-wrap h1 { margin:0; font-weight:800; }
.top-wrap h3 { margin:3px 0 0 0; color:gray; font-weight:600; }
/* 안내 배지 느낌 */
.badge { padding:10px 12px; border-radius:8px; background:#eaf7ef; color:#137333; border:1px solid #cfead6; }
.warn  { padding:10px 12px; border-radius:8px; background:#fff4e5; color:#a15c00; border:1px solid #ffe1b2; }
.note  { font-size:12px; color:#6b7280; margin-top:6px; }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

# ---------------------------
# 로고 (Base64 인코딩; 없으면 생략)
# ---------------------------
def get_base64_of_image(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    except Exception:
        return None

logo_b64 = get_base64_of_image("assets/sempio_logo.png") or get_base64_of_image("sempio_logo.png")

st.markdown(
    f"""
    <div class="top-wrap">
        {'<img src="data:image/png;base64,'+logo_b64+'">' if logo_b64 else ''}
        <div>
            <h1>SEMPIO Global Safety Research</h1>
            <h3>Food Additives Database</h3>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)
st.write("---")

# ---------------------------
# 데이터 로드 유틸 (업로드 기반)
# ---------------------------
@st.cache_data(show_spinner=False)
def read_excel_from_upload(upload) -> pd.DataFrame:
    return pd.read_excel(upload)

def find_column(df: pd.DataFrame, keywords: list[str]) -> str | None:
    for col in df.columns:
        c = str(col).strip().lower()
        for key in keywords:
            if key.lower() in c:
                return col
    return None

# ---------------------------
# 사이드바: DB 업로드
# ---------------------------
st.sidebar.header("데이터 업로드")
st.sidebar.caption("각 DB별 엑셀 1개 업로드 권장 (머지된 파일도 가능)")

kr_up = st.sidebar.file_uploader("KR DB", type=["xlsx", "xls"])
us_up = st.sidebar.file_uploader("US DB", type=["xlsx", "xls"])
eu_up = st.sidebar.file_uploader("EU DB (선택)", type=["xlsx", "xls"])

kr_df = read_excel_from_upload(kr_up) if kr_up else None
us_df = read_excel_from_upload(us_up) if us_up else None
eu_df = read_excel_from_upload(eu_up) if eu_up else None

# 컬럼 정리(공통)
for df in (kr_df, us_df, eu_df):
    if df is not None:
        df.columns = df.columns.map(lambda x: str(x).strip())

# ---------------------------
# 검색창
# ---------------------------
search = st.text_input("원료명 또는 영문명 입력", placeholder="예: 글리신 / glycine / 56-40-6")

# ---------------------------
# Gemini API 설정 (초기화)
# ---------------------------
ENV_KEY = (os.getenv("GOOGLE_API_KEY") or "").strip()
HARDCODED_KEY = "AIzaSyDpPvneo1OyY2a6DUZHgSOWdpcbt9rVx4g"  # API 키 설정 (혹은 환경 변수로 설정)
API_KEY = ENV_KEY if ENV_KEY else HARDCODED_KEY

GEMINI_MODEL = None
CHAT_SESSION = None

if API_KEY:
    try:
        genai.configure(api_key=API_KEY)
        GEMINI_MODEL = genai.GenerativeModel("gemini-2.5-flash")
        CHAT_SESSION = GEMINI_MODEL.start_chat(history=[])
    except Exception as e:
        st.error(f"Gemini API 초기화 실패: {e}")
else:
    st.error("API 키가 없습니다. 환경변수 GOOGLE_API_KEY 또는 HARDCODED_KEY를 설정하세요.")

# ---------------------------
# 검색 실행
# ---------------------------
def filter_df(df: pd.DataFrame, search_txt: str, patterns: list[list[str]]) -> pd.DataFrame:
    """patterns: [[col_keywords...], [col_keywords...], ...]"""
    if df is None or not search_txt:
        return pd.DataFrame()
    # 후보 컬럼 찾기
    cands = []
    for keys in patterns:
        col = find_column(df, keys)
        if col:
            cands.append(col)
    cands = list(dict.fromkeys(cands))  # 중복 제거, 순서 유지

    if not cands:
        return pd.DataFrame()
    mask = False
    for c in cands:
        mask = mask | df[c].astype(str).str.contains(search_txt, case=False, na=False)
    return df.loc[mask].copy()

if search:
    # KR
    kr_res = filter_df(
        kr_df,
        search,
        patterns=[["한글명", "식품첨가물", "additive"],
                  ["영문명", "영문", "english", "substance", "name"],
                  ["cas", "cas no", "cas reg"]]
    )
    # US
    us_res = filter_df(
        us_df,
        search,
        patterns=[["한글명", "additive"],
                  ["영문명", "english", "substance", "name"],
                  ["cas", "cas no", "cas reg"]]
    )
    # EU(선택)
    eu_res = filter_df(
        eu_df,
        search,
        patterns=[["additive_name_en", "name en", "name_en", "additive"],
                  ["cas", "cas list", "cas no", "cas reg"],
                  ["e_number", "e number", "e-number"]]
    )

    # 총 건수 메시지 (스크린샷 느낌)
    total_hits = (0 if kr_res is None else len(kr_res)) + (0 if us_res is None else len(us_res)) + (0 if eu_res is None else len(eu_res))
    st.markdown(f"<div class='badge'>🔎 검색 결과: {total_hits}건이 검색되었습니다.</div>", unsafe_allow_html=True)

    # 탭 구성: KR / US / EU
    tabs = []
    names = []
    if kr_df is not None:
        tabs.append("KR")
        names.append("🇰🇷 KOREA")
    if us_df is not None:
        tabs.append("US")
        names.append("🇺🇸 USA")
    if eu_df is not None:
        tabs.append("EU")
        names.append("🇪🇺 EU")

    if not tabs:
        st.markdown("<div class='warn'>업로드된 DB가 없습니다. 좌측에서 최소 1개 업로드하세요.</div>", unsafe_allow_html=True)
    else:
        t_objs = st.tabs(names)
        ptr = 0

        # ----- KR 탭 -----
        if kr_df is not None:
            with t_objs[ptr]:
                st.subheader("🇰🇷 KOREA")
                if not kr_res.empty:
                    st.dataframe(kr_res, use_container_width=True)
                else:
                    st.info("한국 DB에서 일치 항목이 없습니다.")
            ptr += 1

        # ----- US 탭 -----
        if us_df is not None:
            with t_objs[ptr]:
                st.subheader("🇺🇸 USA")
                if not us_res.empty:
                    st.dataframe(us_res, use_container_width=True)

                    # CFR 링크 열 자동 탐색 후 표 아래 링크 목록 표시
                    us_cfr = find_column(us_res, ["CFR", "reference", "링크", "url", "hyperlink"])
                    us_eng = find_column(us_res, ["영문명", "영문", "english", "substance", "name"])
                    if us_cfr and us_cfr in us_res.columns:
                        st.markdown("---")
                        st.markdown("**CFR Reference Links**")
                        for _, row in us_res.iterrows():
                            name = str(row.get(us_eng, "Unknown"))
                            link = str(row.get(us_cfr, ""))
                            if link.startswith("http"):
                                st.markdown(f"🔗 [{name}]({link})")
                            elif link:
                                st.markdown(f"🔹 {name} - {link}")
                        st.markdown("---")
                else:
                    st.info("미국 DB에서 일치 항목이 없습니다.")
            ptr += 1

        # ----- EU 탭 -----
        if eu_df is not None:
            with t_objs[ptr]:
                st.subheader("🇪🇺 EU")
                if not eu_res.empty:
                    st.dataframe(eu_res, use_container_width=True)
                else:
                    st.info("EU DB에서 일치 항목이 없습니다.")

else:
    st.info("검색할 첨가물명 또는 CAS No.를 입력하세요.")

# ---------------------------
# Gemini 요약 기능 추가 (웹 페이지 요약)
# ---------------------------
def summarize_url_with_gemini(url: str) -> str:
    if not GEMINI_MODEL:
        return "Gemini API가 설정되지 않았습니다."

    try:
        prompt = f"다음 웹 페이지 내용을 요약해 주세요: {url}"
        summary = GEMINI_MODEL.generate_content(prompt).text
        return summary
    except Exception as e:
        return f"오류 발생: {e}"

# 웹페이지 요약
if search and GEMINI_MODEL:
    summary = summarize_url_with_gemini(f"http://example.com/{search}")
    st.markdown("### 요약")
    st.write(summary)

# ---------------------------
# 디버그: 컬럼 확인
# ---------------------------
with st.expander("🔎 열 이름 확인 (클릭하여 보기)"):
    if kr_df is not None:
        st.write("**KR 파일 컬럼명:**", list(kr_df.columns))
    if us_df is not None:
        st.write("**US 파일 컬럼명:**", list(us_df.columns))
    if eu_df is not None:
        st.write("**EU 파일 컬럼명:**", list(eu_df.columns))

st.markdown("<div class='note'>© Sempio • 내부 전용 • </div>", unsafe_allow_html=True)
