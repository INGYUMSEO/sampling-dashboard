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
