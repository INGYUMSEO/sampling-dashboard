# Sampling Dashboard (Streamlit + MySQL)
# - Streamlit 1.28+ / Python 3.10+ / MySQL 8.0+
# - 필요한 비밀값은 .streamlit/secrets.toml (로컬/Cloud) 를 사용합니다.

import io
import json
import pandas as pd
import pymysql
import streamlit as st

st.set_page_config(page_title="표본추출 관리", layout="wide")

# ---------------------- DB 유틸 ----------------------
def get_conn():
    s = st.secrets["mysql"]
    return pymysql.connect(
        host=s["host"],
        port=int(s.get("port", 3306)),
        user=s["user"],
        password=s["password"],
        db=s["db"],
        charset=s.get("charset", "utf8mb4"),
        autocommit=True,  # 프로시저 내 트랜잭션과 충돌 X
        cursorclass=pymysql.cursors.DictCursor,
    )

def query_df(sql, params=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        rows = cur.fetchall()
    return pd.DataFrame(rows)

def exec_proc(sql, params=None):
    """CALL … 형태, 단일 결과셋 반환 전용"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or ())
        try:
            rows = cur.fetchall()
        except Exception:
            rows = []
    return pd.DataFrame(rows)

def call_proc(proc_name, args=()):
    """pymysql.callproc (단일 결과셋)"""
    with get_conn() as conn, conn.cursor() as cur:
        cur.callproc(proc_name, args)
        rows = cur.fetchall()
    return pd.DataFrame(rows)

def call_proc_multi(proc_name, args=()):
    """여러 결과셋 반환 (예: sp_wave_summary) -> [df1, df2, ...]"""
    out = []
    with get_conn() as conn, conn.cursor() as cur:
        cur.callproc(proc_name, args)
        out.append(pd.DataFrame(cur.fetchall()))
        while cur.nextset():
            out.append(pd.DataFrame(cur.fetchall()))
    return out

# ---------------------- 공통: 최신 wave_id ----------------------
def get_last_wave_id():
    df = query_df("SELECT wave_id FROM waves ORDER BY wave_id DESC LIMIT 1;")
    return int(df.iloc[0]["wave_id"]) if len(df) else 0

# ---------------------- 상단 현황 ----------------------
st.title("표본추출 관리 대시보드")

try:
    ver = query_df("SELECT VERSION() AS ver;")
    st.success(f"DB 연결 OK · VERSION: {ver.iloc[0]['ver']}")
except Exception as e:
    st.error(f"DB 연결 실패: {e}")
    st.stop()

pool = query_df("SELECT COUNT(*) AS remaining_pool_size FROM remaining_pool;")
st.metric("Remaining Pool", int(pool.iloc[0]["remaining_pool_size"]) if not pool.empty else 0)

wave_id = get_last_wave_id()
st.caption(f"최근 wave_id: {wave_id}" if wave_id else "아직 생성된 wave 없음")

# ---------------------- 탭 구성 ----------------------
tab_overview, tab_run, tab_upload, tab_ops = st.tabs(
    ["현황/상세", "원클릭 실행", "모집단 업로드", "운영 도구"]
)

# ====================== 현황/상세 ======================
with tab_overview:
    st.subheader("요약")
    if wave_id:
        try:
            res = call_proc_multi("sp_wave_summary", (wave_id,))
            # 기대: [assigned_total, 목표vs실적, remaining_pool_size]
            if len(res) >= 1 and not res[0].empty and "assigned_total" in res[0].columns:
                st.metric("Assigned Total", int(res[0].iloc[0]["assigned_total"]))
            if len(res) >= 2:
                st.write("층화 목표 vs 실적")
                st.dataframe(res[1], use_container_width=True, height=260)
            if len(res) >= 3 and not res[2].empty and "remaining_pool_size" in res[2].columns:
                st.metric("Remaining Pool", int(res[2].iloc[0]["remaining_pool_size"]))
        except Exception as e:
            st.error(f"요약 조회 실패: {e}")
    else:
        st.info("최근 wave가 없습니다. [원클릭 실행]에서 먼저 회차를 생성하세요.")

    st.subheader("배정 상세")
    if wave_id:
        detail = query_df(
            "SELECT * FROM v_assignments_detailed WHERE wave_id=%s ORDER BY assigned_at DESC LIMIT 1000",
            (wave_id,),
        )
        st.dataframe(detail, use_container_width=True, height=340)
        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "CSV 다운로드 (배정 상세)",
                data=detail.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"assignments_wave_{wave_id}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with c2:
            try:
                xbuf = io.BytesIO()
                with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
                    detail.to_excel(w, index=False, sheet_name=f"wave_{wave_id}")
                st.download_button(
                    "XLSX 다운로드 (배정 상세)",
                    data=xbuf.getvalue(),
                    file_name=f"assignments_wave_{wave_id}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            except Exception as e:
                st.warning(f"엑셀 저장 중 경고: {e}")
    else:
        st.info("배정 상세가 없습니다.")

# ====================== 원클릭 실행 ======================
with tab_run:
    st.subheader("회차 생성 → (층화면) 목표 산출 → 추출")

    region_df = query_df("SELECT DISTINCT region FROM population ORDER BY region;")
    region_opts = list(region_df["region"]) if not region_df.empty else ["서울", "경기", "인천", "부산", "대전"]
    gender_opts = ["M", "F"]

    with st.form("one_click_run"):
        c1, c2, c3 = st.columns(3)
        with c1:
            mode = st.selectbox("샘플링 모드", ["STRATIFIED", "RANDOM"])
        with c2:
            target = st.number_input("타깃 N", min_value=1, value=30, step=1)
        with c3:
            seed = st.number_input("Seed(정수)", value=123456, step=1)
        wave_name = st.text_input("회차명", value=f"원클릭_{pd.Timestamp.now().strftime('%m%d_%H%M%S')}")

        st.write("필터(선택):")
        f1, f2, f3 = st.columns(3)
        with f1:
            sel_regions = st.multiselect("지역", region_opts, default=region_opts[: min(3, len(region_opts))])
        with f2:
            sel_genders = st.multiselect("성별", gender_opts, default=gender_opts)
        with f3:
            age_min = st.number_input("최소 나이", value=20, step=1)
            age_max = st.number_input("최대 나이", value=59, step=1)

        ratio_json_text = None
        if mode == "STRATIFIED":
            st.caption(
                "비율 JSON 예시: "
                '[{"key":"서울|M|20s","ratio":0.25},{"key":"서울|F|20s","ratio":0.25}, ...] (합=1)'
            )
            auto_keys = [f"{r}|{g}|{ab}" for r in sel_regions for g in sel_genders for ab in ["20s", "30s", "40s", "50s"]]
            if auto_keys:
                eq = round(1 / len(auto_keys), 6)
                ratio_json_text = json.dumps([{"key": k, "ratio": eq} for k in auto_keys], ensure_ascii=False)
            ratio_json_text = st.text_area("비율 JSON(수정 가능)", value=ratio_json_text or "[]", height=140)

        submit = st.form_submit_button("원클릭 실행")

    if submit:
        try:
            filter_payload = {
                "age_min": int(age_min),
                "age_max": int(age_max),
                "regions": sel_regions if sel_regions else None,
                "genders": sel_genders if sel_genders else None,
            }
            filter_payload = {k: v for k, v in filter_payload.items() if v is not None}
            filter_json = json.dumps(filter_payload, ensure_ascii=False)
            ratio_json = ratio_json_text if mode == "STRATIFIED" else None

            # 회차 생성 (※ sp_create_wave 가 DB에 있어야 합니다)
            df_wave = exec_proc(
                "CALL sp_create_wave(%s,%s,%s,%s,%s,%s,%s)",
                (mode, wave_name, int(target), int(seed), filter_json, ratio_json, "app"),
            )
            if df_wave.empty or "wave_id" not in df_wave.columns:
                st.error("회차 생성 실패(wave_id 반환 없음)")
                st.stop()
            new_wave_id = int(df_wave.iloc[0]["wave_id"])
            st.success(f"회차 생성 완료! wave_id={new_wave_id}")

            # 실행
            if mode == "STRATIFIED":
                st.write("목표 산출 실행…")
                st.dataframe(exec_proc("CALL sp_compute_targets_exactN(%s)", (new_wave_id,)), use_container_width=True)
                st.write("층화 추출 실행…")
                st.dataframe(exec_proc("CALL sp_sample_wave_stratified(%s)", (new_wave_id,)), use_container_width=True)
            else:
                st.write("랜덤 추출 실행…")
                st.dataframe(exec_proc("CALL sp_sample_wave_random(%s)", (new_wave_id,)), use_container_width=True)

            # 요약
            st.subheader("요약")
            res = call_proc_multi("sp_wave_summary", (new_wave_id,))
            if len(res) >= 1 and not res[0].empty:
                st.metric("Assigned Total", int(res[0].iloc[0]["assigned_total"]))
            if len(res) >= 2:
                st.write("층화 목표 vs 실적")
                st.dataframe(res[1], use_container_width=True, height=260)
            if len(res) >= 3 and not res[2].empty:
                st.metric("Remaining Pool", int(res[2].iloc[0]["remaining_pool_size"]))

            # 상세
            st.subheader("배정 상세 (신규 회차)")
            detail_new = query_df(
                "SELECT * FROM v_assignments_detailed WHERE wave_id=%s ORDER BY assigned_at DESC LIMIT 1000",
                (new_wave_id,),
            )
            st.dataframe(detail_new, use_container_width=True, height=320)
            if not detail_new.empty:
                st.download_button(
                    "CSV 다운로드 (신규 회차)",
                    data=detail_new.to_csv(index=False).encode("utf-8-sig"),
                    file_name=f"assignments_wave_{new_wave_id}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            st.session_state["wid"] = new_wave_id
        except Exception as e:
            st.error(f"원클릭 실행 실패: {e}")

# ====================== 모집단 업로드 ======================
with tab_upload:
    st.subheader("모집단 업로드(Excel/CSV) → DB 반영")
    uploaded = st.file_uploader("population 업로드 파일", type=["xlsx", "xls", "csv"])
    mode_ = st.radio("반영 방식", ["대체(모든 데이터 갈아끼움)", "추가(기존에 덧붙임)"], horizontal=True)
    required_cols = ["respondent_id", "phone_raw", "gender", "age", "region"]

    def load_df(file):
        name = file.name.lower()
        if name.endswith(".csv"):
            return pd.read_csv(file)
        return pd.read_excel(file)

    def validate_df(df):
        missing = [c for c in required_cols if c not in df.columns]
        errs = []
        if missing:
            errs.append(f"필수 컬럼 누락: {missing}")
        if "gender" in df.columns:
            vals = set(str(x).upper() for x in df["gender"].dropna().unique())
            if not vals.issubset({"M", "F", "남", "여"}):
                errs.append("gender 값은 M/F(또는 남/여)만 허용")
        return errs

    def normalize_df(df):
        d = df.copy()
        d = d[[c for c in required_cols if c in d.columns]]
        d["gender"] = d["gender"].astype(str).str.upper().replace({"남": "M", "여": "F"})
        d["respondent_id"] = pd.to_numeric(d["respondent_id"], errors="coerce").astype("Int64")
        d["age"] = pd.to_numeric(d["age"], errors="coerce").astype("Int64")
        d = d.dropna(subset=["respondent_id", "gender", "age", "region"])
        d = d[required_cols]
        return d

    def bulk_write(df, replace=False):
        with get_conn() as conn, conn.cursor() as cur:
            if replace:
                cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
                cur.execute("TRUNCATE TABLE assignments;")
                cur.execute("TRUNCATE TABLE exclusions;")
                cur.execute("TRUNCATE TABLE population;")
                cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
            sql = """
                INSERT INTO population(respondent_id, phone_raw, gender, age, region)
                VALUES (%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  phone_raw=VALUES(phone_raw), gender=VALUES(gender),
                  age=VALUES(age), region=VALUES(region)
            """
            rows = list(df.itertuples(index=False, name=None))
            if rows:
                cur.executemany(sql, rows)

    if uploaded:
        df_raw = load_df(uploaded)
        st.caption("미리보기(상위 200행)")
        st.dataframe(df_raw.head(200), use_container_width=True, height=260)
        errs = validate_df(df_raw)
        if errs:
            st.error(" / ".join(errs))
        else:
            df_norm = normalize_df(df_raw)
            st.success(f"검증 OK · 반영 대상 행: {len(df_norm):,}행")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("DB 반영", type="primary", use_container_width=True):
                    bulk_write(df_norm, replace=mode_.startswith("대체"))
                    pool2 = query_df("SELECT COUNT(*) AS remaining_pool_size FROM remaining_pool;")
                    st.success(f"DB 반영 완료 · Remaining Pool: {int(pool2.iloc[0]['remaining_pool_size']):,}")
            with c2:
                try:
                    xbuf = io.BytesIO()
                    with pd.ExcelWriter(xbuf, engine="openpyxl") as w:
                        df_norm.to_excel(w, index=False, sheet_name="population")
                    st.download_button(
                        "업로드 정규화본 XLSX 저장",
                        data=xbuf.getvalue(),
                        file_name="population_normalized.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.warning(f"엑셀 저장 중 경고: {e}")

# ====================== 운영 도구 (A)~(C) ======================
with tab_ops:
    st.subheader("A) 부족분 자동 보충 (Top-up)")
    wid_top = st.number_input("wave_id", min_value=1, step=1, value=wave_id or 1)
    if st.button("Top-up 실행"):
        try:
            before = call_proc_multi("sp_wave_summary", (int(wid_top),))
            st.write("Before")
            if before and len(before) >= 2 and not before[0].empty:
                st.metric("Assigned Total", int(before[0].iloc[0]["assigned_total"]))
                st.dataframe(before[1], use_container_width=True, height=240)
            res = call_proc("sp_wave_topup", (int(wid_top),))
            st.success("Top-up 완료")
            after = call_proc_multi("sp_wave_summary", (int(wid_top),))
            st.write("After")
            if after and len(after) >= 2 and not after[0].empty:
                st.metric("Assigned Total", int(after[0].iloc[0]["assigned_total"]))
                st.dataframe(after[1], use_container_width=True, height=240)
                if len(after) >= 3 and not after[2].empty:
                    st.metric("Remaining Pool", int(after[2].iloc[0]["remaining_pool_size"]))
        except Exception as e:
            st.error(f"Top-up 실패: {e}")

    st.markdown("---")
    st.subheader("B) 실행 로그/감사 래퍼 (층화 전체 실행)")
    wid_run = st.number_input("wave_id(층화)", min_value=1, step=1, value=wave_id or 1, key="wid_run")
    actor = st.text_input("Actor", value="app")
    if st.button("층화 전체 실행 & runs 기록"):
        try:
            res = call_proc("sp_run_stratified_full", (int(wid_run), actor))
            st.dataframe(res, use_container_width=True)
            st.write("최근 runs")
            runs = query_df("SELECT * FROM runs ORDER BY run_id DESC LIMIT 10;")
            st.dataframe(runs, use_container_width=True)
        except Exception as e:
            st.error(f"실행 실패: {e}")

    st.markdown("---")
    st.subheader("C) 샘플 데이터 로더 (초기화 + N명)")
    gen_n = st.number_input("생성 인원", min_value=100, max_value=200000, value=8000, step=100)
    if st.button("초기화 후 N명 생성", type="primary"):
        try:
            call_proc("sp_reset_and_load_population", (int(gen_n),))
            pool2 = query_df("SELECT COUNT(*) AS remaining_pool_size FROM remaining_pool;")
            st.success(f"생성 완료! Remaining Pool: {int(pool2.iloc[0]['remaining_pool_size']):,}")
        except Exception as e:
            st.error(f"샘플 생성 실패: {e}")
