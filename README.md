# Sampling Dashboard (Streamlit + MySQL)

응답자 조사에서 모집단 → 표본 추출을 돕는 대시보드입니다.  
엑셀 업로드/대체, 원클릭 실행(회차 생성→목표 산출→추출), 부족분 Top-up, 실행 로그까지 제공합니다.

![stack](https://img.shields.io/badge/Streamlit-1.38%2B-red)
![stack](https://img.shields.io/badge/MySQL-8%2B-blue)
![stack](https://img.shields.io/badge/Python-3.10%2B-green)

---

## ✨ 주요 기능

- **원클릭 실행**: 회차 생성 → (층화면) 목표 산출 → 추출까지 한 번에
- **층화/랜덤 추출**: 비율 JSON 기반 **정확 N** 목표 산출 또는 랜덤 N 추출
- **부족분 Top-up**: 목표 대비 부족한 셀만 자동 추가 배정
- **모집단 업로드**: Excel/CSV 업로드, **대체/추가** 모드 지원
- **배정 상세 다운로드**: CSV/XLSX
- **실행 로그(Runs)**: 누가/언제/무엇을 실행했는지 기록
- **remaining_pool 뷰**: 배정/배제 제외 대상 자동 반영

---

## 🧱 요구사항

- **OS**: Windows / macOS / Linux  
- **Python**: 3.10 이상  
- **MySQL**: 8.0 이상 (JSON, 윈도우 함수 사용)  
- **필수 패키지**: `streamlit`, `pymysql`, `pandas`, `openpyxl`  
  → `pip install -r requirements.txt`

---

## 📦 설치 & 로컬 실행
### 1) 가상환경 & 패키지

> Python 3.10+ 권장. PowerShell은 `ExecutionPolicy` 때문에 활성화 오류가 나면 아래 참고를 확인하세요.

**Windows (PowerShell)**

```powershell
# 가상환경 생성
python -m venv .venv

# 활성화
.\.venv\Scripts\Activate.ps1
# (필요 시) 일시적으로 스크립트 허용:
# Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# 패키지 설치
pip install -r requirements.txt

### macOS / Linux 설치 과정

```bash
# 가상환경 생성
python3 -m venv .venv

# 활성화
source .venv/bin/activate

# 패키지 설치
pip install -r requirements.txt

``markdown
## 🧭 사용법 요약

### 탭 구성

#### 현황/상세
- 최근 회차의 요약 지표와 **배정 상세표**를 보여줍니다.
- 결과를 **CSV / XLSX**로 다운로드할 수 있습니다.

#### 원클릭 실행
- 모드 선택: **STRATIFIED(층화)** 또는 **RANDOM(랜덤)**.
- **타깃 N**, **Seed** 지정.
- (선택) **필터**: 지역 / 성별 / 나이 범위.
- `STRATIFIED`인 경우 **비율 JSON**을 입력하면 셀별 **정확 N**이 계산됩니다.
- **원클릭 실행** 버튼을 누르면  
  **회차 생성 → (층화면) 목표 산출 → 추출**이 순차적으로 실행됩니다.

#### 모집단 업로드
- **Excel/CSV** 파일 업로드. 필수 컬럼:
  - `respondent_id, phone_raw, gender, age, region`
- **대체** 모드: 기존 `population / assignments / exclusions` 초기화 후 반영.
- **추가** 모드: 기존 데이터에 덧붙임(동일 ID는 업데이트).
- 업로드 후 **미리보기 → 검증 → DB 반영** → Remaining Pool 갱신 메시지 확인.

#### 운영 도구
- **A) 부족분 자동 보충 (Top-up)**: 특정 `wave_id`의 목표 대비 **부족한 셀만** 추가 배정.
- **B) 실행 로그/감사 래퍼**: `sp_compute_targets_exactN → sp_sample_wave_stratified`를 **runs** 테이블에 기록하며 실행.
- **C) 샘플 데이터 로더**: 테스트용 모집단을 **N명 생성**(외래키 안전 **루프 버전**).

---

### 빠른 시연(스모크 테스트)

1. **운영 도구 → C) 샘플 데이터 로더**에서 `N=8000` 생성  
   → 상단 **Remaining Pool** 숫자(예: 8000) 확인.
2. **원클릭 실행**
   - 모드: `STRATIFIED`, 타깃 N: `120`, Seed: `123456`
   - 지역/성별/나이 필터 선택 → 기본 제공 **균등 비율 JSON** 그대로 사용 → **원클릭 실행**
   - “회차 생성 완료! `wave_id=…`” 메시지와 **요약/배정 상세**가 출력되면 OK.
3. **현황/상세** 탭에서 **배정 상세 CSV/XLSX** 다운로드로 결과 확인.
