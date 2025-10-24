USE sampling;

-- ------------------------------------------------------------
-- 0) 대시보드용 뷰 재정의
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW remaining_pool AS
SELECT p.*
FROM population p
LEFT JOIN assignments a ON a.respondent_id = p.respondent_id
LEFT JOIN exclusions  e ON e.respondent_id = p.respondent_id
WHERE a.respondent_id IS NULL
  AND e.respondent_id IS NULL;

CREATE OR REPLACE VIEW v_assignments_detailed AS
SELECT a.wave_id, a.respondent_id, p.phone_raw, p.gender, p.age, p.region,
       CONCAT(p.region,'|',p.gender,'|',
         CASE WHEN p.age BETWEEN 10 AND 19 THEN '10s'
              WHEN p.age BETWEEN 20 AND 29 THEN '20s'
              WHEN p.age BETWEEN 30 AND 39 THEN '30s'
              WHEN p.age BETWEEN 40 AND 49 THEN '40s'
              WHEN p.age BETWEEN 50 AND 59 THEN '50s' ELSE '60s+' END
       ) AS stratum_key,
       a.assigned_at
FROM assignments a
JOIN population p ON p.respondent_id = a.respondent_id;

-- ------------------------------------------------------------
-- 1) 기존 프로시저 정리
-- ------------------------------------------------------------
DROP PROCEDURE IF EXISTS sp_create_wave;
DROP PROCEDURE IF EXISTS sp_compute_targets_exactN;
DROP PROCEDURE IF EXISTS sp_sample_wave_stratified;
DROP PROCEDURE IF EXISTS sp_sample_wave_random;
DROP PROCEDURE IF EXISTS sp_wave_summary;
DROP PROCEDURE IF EXISTS sp_wave_topup;
DROP PROCEDURE IF EXISTS sp_run_stratified_full;
DROP PROCEDURE IF EXISTS sp_wave_delete;
DROP PROCEDURE IF EXISTS sp_exclude_bulk;
DROP PROCEDURE IF EXISTS sp_reset_and_load_population;

DELIMITER $$

-- ------------------------------------------------------------
-- 2) 회차 생성
--    waves(mode, wave_name, target_count, seed_value, filter_json, ratio_json, created_by)
-- ------------------------------------------------------------
CREATE PROCEDURE sp_create_wave(
  IN p_mode ENUM('STRATIFIED','RANDOM'),
  IN p_wave_name VARCHAR(200),
  IN p_target INT,
  IN p_seed BIGINT,
  IN p_filter JSON,
  IN p_ratio JSON,
  IN p_actor VARCHAR(100)
)
BEGIN
  INSERT INTO waves(mode, wave_name, target_count, seed_value, filter_json, ratio_json, created_by)
  VALUES(p_mode, p_wave_name, p_target, p_seed, p_filter, p_ratio, p_actor);

  SELECT LAST_INSERT_ID() AS wave_id;
END $$

-- ------------------------------------------------------------
-- 3) 정확 N 목표 산출 (층화)
--    ratio_json 제공 시 비율대로, 없으면 균등분할 후 잔여치 보정
-- ------------------------------------------------------------
CREATE PROCEDURE sp_compute_targets_exactN(IN p_wave_id BIGINT)
BEGIN
  DECLARE v_target INT;
  DECLARE v_ratio LONGTEXT;
  DECLARE v_filter LONGTEXT;

  SELECT target_count, ratio_json, filter_json
    INTO v_target, v_ratio, v_filter
  FROM waves WHERE wave_id = p_wave_id;

  DELETE FROM wave_strata_targets WHERE wave_id = p_wave_id;

  IF v_ratio IS NULL OR JSON_LENGTH(v_ratio) = 0 THEN
    -- 후보 strata 수집(필터 반영된 remaining_pool 기준)
    CREATE TEMPORARY TABLE tmp_strata AS
    SELECT DISTINCT
      CONCAT(p.region,'|',p.gender,'|',
        CASE
          WHEN p.age BETWEEN 10 AND 19 THEN '10s'
          WHEN p.age BETWEEN 20 AND 29 THEN '20s'
          WHEN p.age BETWEEN 30 AND 39 THEN '30s'
          WHEN p.age BETWEEN 40 AND 49 THEN '40s'
          WHEN p.age BETWEEN 50 AND 59 THEN '50s'
          ELSE '60s+' END
      ) AS stratum_key
    FROM remaining_pool p
    WHERE
      (JSON_EXTRACT(v_filter,'$.age_min') IS NULL OR p.age >= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_min')) AS SIGNED))
      AND (JSON_EXTRACT(v_filter,'$.age_max') IS NULL OR p.age <= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_max')) AS SIGNED))
      AND (JSON_EXTRACT(v_filter,'$.regions') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.regions'), JSON_QUOTE(p.region)))
      AND (JSON_EXTRACT(v_filter,'$.genders') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.genders'), JSON_QUOTE(p.gender)));

    SET @k := (SELECT COUNT(*) FROM tmp_strata);

    INSERT INTO wave_strata_targets(wave_id, stratum_key, target_n)
    SELECT p_wave_id, stratum_key,
           CASE WHEN @k>0 THEN FLOOR(v_target/@k) ELSE 0 END
    FROM tmp_strata;

    -- 잔여치 보정
    SET @diff := v_target - (SELECT COALESCE(SUM(target_n),0) FROM wave_strata_targets WHERE wave_id=p_wave_id);
    IF @diff <> 0 THEN
      UPDATE wave_strata_targets
      SET target_n = target_n + 1
      WHERE wave_id = p_wave_id
      ORDER BY stratum_key
      LIMIT @diff;
    END IF;

    DROP TEMPORARY TABLE IF EXISTS tmp_strata;

  ELSE
    -- ratio_json 사용(JSON_TABLE 필요: MySQL 8.0.13+)
    INSERT INTO wave_strata_targets(wave_id, stratum_key, target_n)
    SELECT p_wave_id, jt.key, CAST(ROUND(jt.ratio * v_target) AS SIGNED)
    FROM JSON_TABLE(v_ratio, '$[*]'
           COLUMNS(
             `key`   VARCHAR(100) PATH '$.key',
             `ratio` DECIMAL(10,6) PATH '$.ratio'
           )) AS jt;

    -- 합 보정(반올림 오차)
    SET @diff := v_target - (SELECT COALESCE(SUM(target_n),0) FROM wave_strata_targets WHERE wave_id=p_wave_id);
    IF @diff <> 0 THEN
      UPDATE wave_strata_targets
      SET target_n = target_n + CASE WHEN @diff>0 THEN 1 ELSE -1 END
      WHERE wave_id = p_wave_id
      ORDER BY stratum_key
      LIMIT ABS(@diff);
    END IF;
  END IF;

  SELECT * FROM wave_strata_targets WHERE wave_id = p_wave_id;
END $$

-- ------------------------------------------------------------
-- 4) 층화 추출
-- ------------------------------------------------------------
CREATE PROCEDURE sp_sample_wave_stratified(IN p_wave_id BIGINT)
BEGIN
  DECLARE v_seed   BIGINT;
  DECLARE v_filter LONGTEXT;

  SELECT seed_value, filter_json INTO v_seed, v_filter
  FROM waves WHERE wave_id = p_wave_id;

  CREATE TEMPORARY TABLE tmp_cand AS
  SELECT
    p.respondent_id,
    CONCAT(p.region,'|',p.gender,'|',
      CASE
        WHEN p.age BETWEEN 10 AND 19 THEN '10s'
        WHEN p.age BETWEEN 20 AND 29 THEN '20s'
        WHEN p.age BETWEEN 30 AND 39 THEN '30s'
        WHEN p.age BETWEEN 40 AND 49 THEN '40s'
        WHEN p.age BETWEEN 50 AND 59 THEN '50s'
        ELSE '60s+' END
    ) AS stratum_key,
    CRC32(CONCAT(v_seed,'-',p.respondent_id)) AS rk
  FROM remaining_pool p
  WHERE
    (JSON_EXTRACT(v_filter,'$.age_min') IS NULL OR p.age >= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_min')) AS SIGNED))
    AND (JSON_EXTRACT(v_filter,'$.age_max') IS NULL OR p.age <= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_max')) AS SIGNED))
    AND (JSON_EXTRACT(v_filter,'$.regions') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.regions'), JSON_QUOTE(p.region)))
    AND (JSON_EXTRACT(v_filter,'$.genders') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.genders'), JSON_QUOTE(p.gender)));

  INSERT IGNORE INTO assignments(wave_id, respondent_id)
  SELECT p_wave_id, x.respondent_id
  FROM (
    SELECT respondent_id, stratum_key,
           ROW_NUMBER() OVER (PARTITION BY stratum_key ORDER BY rk) AS rn
    FROM tmp_cand
  ) x
  JOIN wave_strata_targets t
    ON t.wave_id = p_wave_id AND t.stratum_key = x.stratum_key
  WHERE x.rn <= t.target_n;

  DROP TEMPORARY TABLE IF EXISTS tmp_cand;

  SELECT COUNT(*) AS assigned_total
  FROM assignments
  WHERE wave_id = p_wave_id;
END $$

-- ------------------------------------------------------------
-- 5) 랜덤 추출
-- ------------------------------------------------------------
CREATE PROCEDURE sp_sample_wave_random(IN p_wave_id BIGINT)
BEGIN
  DECLARE v_seed BIGINT;
  DECLARE v_target INT;
  DECLARE v_filter LONGTEXT;

  SELECT seed_value, target_count, filter_json
    INTO v_seed, v_target, v_filter
  FROM waves WHERE wave_id = p_wave_id;

  CREATE TEMPORARY TABLE tmp_c AS
  SELECT p.respondent_id, CRC32(CONCAT(v_seed,'-',p.respondent_id)) AS rk
  FROM remaining_pool p
  WHERE
    (JSON_EXTRACT(v_filter,'$.age_min') IS NULL OR p.age >= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_min')) AS SIGNED))
    AND (JSON_EXTRACT(v_filter,'$.age_max') IS NULL OR p.age <= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_max')) AS SIGNED))
    AND (JSON_EXTRACT(v_filter,'$.regions') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.regions'), JSON_QUOTE(p.region)))
    AND (JSON_EXTRACT(v_filter,'$.genders') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.genders'), JSON_QUOTE(p.gender)));

  INSERT IGNORE INTO assignments(wave_id, respondent_id)
  SELECT p_wave_id, respondent_id
  FROM tmp_c
  ORDER BY rk
  LIMIT v_target;

  DROP TEMPORARY TABLE IF EXISTS tmp_c;

  SELECT COUNT(*) AS assigned_total
  FROM assignments
  WHERE wave_id = p_wave_id;
END $$

-- ------------------------------------------------------------
-- 6) 회차 요약
-- ------------------------------------------------------------
CREATE PROCEDURE sp_wave_summary(IN p_wave_id BIGINT)
BEGIN
  -- 총 배정 수
  SELECT COUNT(*) AS assigned_total
  FROM assignments
  WHERE wave_id = p_wave_id;

  -- 층화 목표 vs 실적
  SELECT
    t.stratum_key,
    t.target_n,
    COALESCE(a.actual_n,0) AS actual_n,
    (t.target_n - COALESCE(a.actual_n,0)) AS shortage
  FROM wave_strata_targets t
  LEFT JOIN (
    SELECT
      CONCAT(p.region,'|',p.gender,'|',
        CASE
          WHEN p.age BETWEEN 10 AND 19 THEN '10s'
          WHEN p.age BETWEEN 20 AND 29 THEN '20s'
          WHEN p.age BETWEEN 30 AND 39 THEN '30s'
          WHEN p.age BETWEEN 40 AND 49 THEN '40s'
          WHEN p.age BETWEEN 50 AND 59 THEN '50s'
          ELSE '60s+' END
      ) AS stratum_key,
      COUNT(*) AS actual_n
    FROM assignments a
    JOIN population p ON p.respondent_id = a.respondent_id
    WHERE a.wave_id = p_wave_id
    GROUP BY 1
  ) a ON a.stratum_key = t.stratum_key
  WHERE t.wave_id = p_wave_id;

  -- 잔여 풀
  SELECT COUNT(*) AS remaining_pool_size FROM remaining_pool;
END $$

-- ------------------------------------------------------------
-- 7) 부족분 자동 보충(임시테이블 재오픈 방지)
-- ------------------------------------------------------------
CREATE PROCEDURE sp_wave_topup(IN p_wave_id BIGINT)
BEGIN
  DECLARE v_seed BIGINT; DECLARE v_filter LONGTEXT;
  SELECT seed_value, filter_json INTO v_seed, v_filter FROM waves WHERE wave_id=p_wave_id;

  CREATE TEMPORARY TABLE tmp_need AS
  SELECT t.stratum_key,
         t.target_n,
         COALESCE(a.actual_n,0) AS actual_n,
         GREATEST(t.target_n-COALESCE(a.actual_n,0),0) AS add_n
  FROM wave_strata_targets t
  LEFT JOIN (
    SELECT CONCAT(p.region,'|',p.gender,'|',
           CASE WHEN p.age BETWEEN 10 AND 19 THEN '10s'
                WHEN p.age BETWEEN 20 AND 29 THEN '20s'
                WHEN p.age BETWEEN 30 AND 39 THEN '30s'
                WHEN p.age BETWEEN 40 AND 49 THEN '40s'
                WHEN p.age BETWEEN 50 AND 59 THEN '50s' ELSE '60s+' END) AS stratum_key,
           COUNT(*) AS actual_n
    FROM assignments a JOIN population p ON p.respondent_id = a.respondent_id
    WHERE a.wave_id = p_wave_id GROUP BY 1
  ) a ON a.stratum_key = t.stratum_key
  WHERE t.wave_id = p_wave_id AND (t.target_n-COALESCE(a.actual_n,0))>0;

  CREATE TEMPORARY TABLE tmp_need2 AS SELECT * FROM tmp_need;

  CREATE TEMPORARY TABLE tmp_cand AS
  SELECT p.respondent_id,
         CONCAT(p.region,'|',p.gender,'|',
           CASE WHEN p.age BETWEEN 10 AND 19 THEN '10s'
                WHEN p.age BETWEEN 20 AND 29 THEN '20s'
                WHEN p.age BETWEEN 30 AND 39 THEN '30s'
                WHEN p.age BETWEEN 40 AND 49 THEN '40s'
                WHEN p.age BETWEEN 50 AND 59 THEN '50s' ELSE '60s+' END) AS stratum_key,
         CRC32(CONCAT(v_seed,'-',p.respondent_id)) AS rk
  FROM remaining_pool p
  WHERE
    (JSON_EXTRACT(v_filter,'$.age_min') IS NULL OR p.age >= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_min')) AS SIGNED))
    AND (JSON_EXTRACT(v_filter,'$.age_max') IS NULL OR p.age <= CAST(JSON_UNQUOTE(JSON_EXTRACT(v_filter,'$.age_max')) AS SIGNED))
    AND (JSON_EXTRACT(v_filter,'$.regions') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.regions'), JSON_QUOTE(p.region)))
    AND (JSON_EXTRACT(v_filter,'$.genders') IS NULL OR JSON_CONTAINS(JSON_EXTRACT(v_filter,'$.genders'), JSON_QUOTE(p.gender)));

  INSERT IGNORE INTO assignments(wave_id, respondent_id)
  SELECT p_wave_id, x.respondent_id
  FROM (
    SELECT c.respondent_id, c.stratum_key,
           ROW_NUMBER() OVER (PARTITION BY c.stratum_key ORDER BY c.rk) AS rn
    FROM tmp_cand c
    JOIN tmp_need n1 ON n1.stratum_key=c.stratum_key
  ) x
  JOIN tmp_need2 n2 ON n2.stratum_key=x.stratum_key
  WHERE x.rn <= n2.add_n;

  DROP TEMPORARY TABLE IF EXISTS tmp_need;
  DROP TEMPORARY TABLE IF EXISTS tmp_need2;
  DROP TEMPORARY TABLE IF EXISTS tmp_cand;

  SELECT COUNT(*) AS assigned_total FROM assignments WHERE wave_id=p_wave_id;
END $$

-- ------------------------------------------------------------
-- 8) 실행 로그/감사 래퍼(층화 풀 파이프)
-- ------------------------------------------------------------
CREATE PROCEDURE sp_run_stratified_full(IN p_wave_id BIGINT, IN p_actor VARCHAR(100))
BEGIN
  DECLARE v_run_id BIGINT;
  INSERT INTO runs(wave_id, actor, params_json)
  VALUES(p_wave_id, p_actor, JSON_OBJECT('flow','stratified_full'));
  SET v_run_id = LAST_INSERT_ID();

  CALL sp_compute_targets_exactN(p_wave_id);
  CALL sp_sample_wave_stratified(p_wave_id);

  UPDATE runs
     SET finished_at = NOW(),
         result_json = JSON_OBJECT('assigned_total',
           (SELECT COUNT(*) FROM assignments WHERE wave_id=p_wave_id))
   WHERE run_id = v_run_id;

  SELECT v_run_id AS run_id;
END $$

-- ------------------------------------------------------------
-- 9) 회차 롤백 / 배제 일괄
-- ------------------------------------------------------------
CREATE PROCEDURE sp_wave_delete(IN p_wave_id BIGINT)
BEGIN
  DELETE FROM assignments         WHERE wave_id = p_wave_id;
  DELETE FROM wave_strata_targets WHERE wave_id = p_wave_id;
  DELETE FROM waves               WHERE wave_id = p_wave_id;
END $$

CREATE PROCEDURE sp_exclude_bulk(IN p_reason VARCHAR(200))
BEGIN
  -- 사전 준비: tmp_excl_ids(respondent_id) 임시테이블에 대상 업로드
  INSERT IGNORE INTO exclusions(respondent_id, reason)
  SELECT respondent_id, p_reason FROM tmp_excl_ids;
END $$

-- ------------------------------------------------------------
-- 10) 샘플 데이터 로더(초기화 + N명 생성, LOOP)
-- ------------------------------------------------------------
CREATE PROCEDURE sp_reset_and_load_population(IN p_count INT)
BEGIN
  DECLARE i INT DEFAULT 1;

  SET FOREIGN_KEY_CHECKS = 0;
  TRUNCATE TABLE assignments;
  TRUNCATE TABLE exclusions;
  TRUNCATE TABLE population;
  SET FOREIGN_KEY_CHECKS = 1;

  START TRANSACTION;
  WHILE i <= p_count DO
    INSERT INTO population (respondent_id, phone_raw, gender, age, region)
    VALUES (
      i,
      CONCAT('010-', LPAD(FLOOR(RAND(i)*9000)+1000,4,'0'), '-',
                    LPAD(FLOOR(RAND(i*17)*9000)+1000,4,'0')),
      IF(RAND(i*3) < 0.5, 'M','F'),
      20 + FLOOR(RAND(i*5) * 40),
      ELT(1 + FLOOR(RAND(i*11) * 5), '서울','경기','인천','부산','대전')
    );
    SET i = i + 1;
  END WHILE;
  COMMIT;
END $$

DELIMITER ;
