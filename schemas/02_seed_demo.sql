USE sampling;

/* 데모 초기화 + 8,000명 생성 */
CALL sp_reset_and_load_population(8000);

/* 데모 회차 생성(층화) */
SET @filter = JSON_OBJECT('age_min',20,'age_max',59,'regions',JSON_ARRAY('서울','경기','인천','부산','대전'),'genders',JSON_ARRAY('M','F'));
SET @ratio  = JSON_ARRAY(
  JSON_OBJECT('key','서울|M|20s','ratio',0.10),
  JSON_OBJECT('key','서울|F|20s','ratio',0.10),
  JSON_OBJECT('key','경기|M|30s','ratio',0.10),
  JSON_OBJECT('key','경기|F|30s','ratio',0.10),
  JSON_OBJECT('key','인천|M|40s','ratio',0.10),
  JSON_OBJECT('key','인천|F|40s','ratio',0.10),
  JSON_OBJECT('key','부산|M|50s','ratio',0.10),
  JSON_OBJECT('key','부산|F|50s','ratio',0.10),
  JSON_OBJECT('key','대전|M|20s','ratio',0.10),
  JSON_OBJECT('key','대전|F|20s','ratio',0.10)
);

CALL sp_create_wave('STRATIFIED','데모_회차',120,123456,@filter,@ratio,'seed');
SET @wid = (SELECT LAST_INSERT_ID());

CALL sp_compute_targets_exactN(@wid);
CALL sp_sample_wave_stratified(@wid);
CALL sp_wave_summary(@wid);
