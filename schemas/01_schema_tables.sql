-- 실행 전: 스키마 선택
-- CREATE DATABASE IF NOT EXISTS sampling DEFAULT CHARSET utf8mb4;
USE sampling;

-- =========================
-- SCHEMA: tables & views
-- =========================

-- 모집단
CREATE TABLE IF NOT EXISTS population (
  respondent_id  BIGINT       NOT NULL PRIMARY KEY,
  phone_raw      VARCHAR(32),
  gender         ENUM('M','F') NOT NULL,
  age            INT           NOT NULL,
  region         VARCHAR(64)   NOT NULL,
  INDEX idx_pop_region (region),
  INDEX idx_pop_gender (gender),
  INDEX idx_pop_age (age)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 회차 메타
CREATE TABLE IF NOT EXISTS waves (
  wave_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
  mode          ENUM('STRATIFIED','RANDOM') NOT NULL,
  wave_name     VARCHAR(200) NOT NULL,
  target_count  INT NOT NULL,
  seed_value    BIGINT NOT NULL,
  filter_json   JSON NULL,
  ratio_json    JSON NULL,
  created_by    VARCHAR(100),
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 층화 목표(정확 N)  ← 이름 오타 수정: wave_strata_targets
CREATE TABLE IF NOT EXISTS wave_strata_targets (
  wave_id     BIGINT NOT NULL,
  stratum_key VARCHAR(64) NOT NULL,         -- e.g. "서울|M|20s"
  target_n    INT NOT NULL,
  PRIMARY KEY (wave_id, stratum_key),
  CONSTRAINT fk_wst_wave FOREIGN KEY (wave_id)
    REFERENCES waves(wave_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 배정 결과
CREATE TABLE IF NOT EXISTS assignments (
  wave_id       BIGINT NOT NULL,
  respondent_id BIGINT NOT NULL,
  assigned_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (wave_id, respondent_id),
  CONSTRAINT fk_asg_wave FOREIGN KEY (wave_id)
    REFERENCES waves(wave_id) ON DELETE CASCADE,
  CONSTRAINT fk_asg_pop  FOREIGN KEY (respondent_id)
    REFERENCES population(respondent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 배제 목록
CREATE TABLE IF NOT EXISTS exclusions (
  respondent_id BIGINT NOT NULL PRIMARY KEY,
  reason        VARCHAR(255),
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT fk_exc_pop FOREIGN KEY (respondent_id)
    REFERENCES population(respondent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 실행 로그
CREATE TABLE IF NOT EXISTS runs (
  run_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
  wave_id     BIGINT NULL,
  actor       VARCHAR(100),
  params_json JSON NULL,                    -- 컬럼명 일치(앱/프로시저와 동일)
  result_json JSON NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  finished_at DATETIME NULL,
  CONSTRAINT fk_runs_wave FOREIGN KEY (wave_id)
    REFERENCES waves(wave_id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================
-- Views (뷰)
-- =========================

-- 잔여 풀: 배정/배제 제외
DROP VIEW IF EXISTS remaining_pool;
CREATE ALGORITHM=MERGE VIEW remaining_pool AS
SELECT p.*
FROM population p
LEFT JOIN assignments a ON a.respondent_id = p.respondent_id
LEFT JOIN exclusions  e ON e.respondent_id = p.respondent_id
WHERE a.respondent_id IS NULL
  AND e.respondent_id IS NULL;

-- 배정 상세(다운로드/리포팅)
DROP VIEW IF EXISTS v_assignments_detailed;
CREATE ALGORITHM=MERGE VIEW v_assignments_detailed AS
SELECT
  a.wave_id,
  a.respondent_id,
  p.phone_raw,
  p.gender,
  p.age,
  p.region,
  CONCAT(
    p.region,'|',p.gender,'|',
    CASE
      WHEN p.age BETWEEN 10 AND 19 THEN '10s'
      WHEN p.age BETWEEN 20 AND 29 THEN '20s'
      WHEN p.age BETWEEN 30 AND 39 THEN '30s'
      WHEN p.age BETWEEN 40 AND 49 THEN '40s'
      WHEN p.age BETWEEN 50 AND 59 THEN '50s'
      ELSE '60s+'
    END
  ) AS stratum_key,
  a.assigned_at                      
FROM assignments a
JOIN population p ON p.respondent_id = a.respondent_id;

