USE final;

-- 출석 테이블
SELECT *
FROM accounts_attendance;

SELECT COUNT(*) AS null_attendance
FROM accounts_attendance
WHERE attendance_date_list IS NULL;

SELECT COUNT(*) AS empty_attendance
FROM accounts_attendance
WHERE attendance_date_list = '[]';

-- 날짜 나누기 / 최초, 마지막 날짜 확인
SELECT
  a.user_id,
  MIN(STR_TO_DATE(jt.att_date, '%Y-%m-%d')) AS first_attendance_date,
  MAX(STR_TO_DATE(jt.att_date, '%Y-%m-%d')) AS last_attendance_date
FROM accounts_attendance a
JOIN JSON_TABLE(
  CAST(
    REPLACE(a.attendance_date_list, '\\\"', '"')
    AS JSON
  ),
  '$[*]' COLUMNS (
    att_date VARCHAR(10) PATH '$'
  )
) jt
GROUP BY a.user_id;

SELECT
  MIN(STR_TO_DATE(jt.att_date, '%Y-%m-%d')) AS global_first_attendance_date,
  MAX(STR_TO_DATE(jt.att_date, '%Y-%m-%d')) AS global_last_attendance_date
FROM accounts_attendance a
JOIN JSON_TABLE(
  CAST(
    REPLACE(a.attendance_date_list, '\\\"', '"')
    AS JSON
  ),
  '$[*]' COLUMNS (
    att_date VARCHAR(10) PATH '$'
  )
) jt;

-- 월별로 출석현황
WITH RECURSIVE cleaned AS (
  SELECT
    user_id,
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(attendance_date_list, '\\\"', '"'),
        '[', ''),
      ']', ''),
    '"', '') AS s
  FROM accounts_attendance
  WHERE attendance_date_list IS NOT NULL
),
split AS (
  SELECT
    user_id,
    TRIM(SUBSTRING_INDEX(s, ',', 1)) AS att_date,
    CASE
      WHEN INSTR(s, ',') > 0 THEN SUBSTRING(s, INSTR(s, ',') + 1)
      ELSE ''
    END AS rest
  FROM cleaned

  UNION ALL

  SELECT
    user_id,
    TRIM(SUBSTRING_INDEX(rest, ',', 1)) AS att_date,
    CASE
      WHEN INSTR(rest, ',') > 0 THEN SUBSTRING(rest, INSTR(rest, ',') + 1)
      ELSE ''
    END AS rest
  FROM split
  WHERE rest <> ''
)
SELECT
  DATE_FORMAT(STR_TO_DATE(att_date, '%Y-%m-%d'), '%Y-%m') AS ym,
  COUNT(*) AS attendance_cnt
FROM split
WHERE att_date <> ''
GROUP BY ym
ORDER BY ym;

-- 차단 기록 테이블
SELECT *
FROM accounts_blockrecord;

SELECT DISTINCT reason
FROM accounts_blockrecord;

-- 상품 구매 실패 기록 테이블
SELECT COUNT(DISTINCT user_id)
FROM accounts_failpaymenthistory;

SELECT DISTINCT productId
FROM accounts_failpaymenthistory;

SELECT DISTINCT phone_type
FROM accounts_failpaymenthistory;


-- 친구 요청 테이블
SELECT *
FROM accounts_friendrequest;

SELECT DISTINCT `status`
FROM accounts_friendrequest; 

-- 학급 테이블
SELECT *
FROM accounts_group;

SELECT COUNT(DISTINCT school_id)
FROM accounts_group;

SELECT COUNT(DISTINCT id)
FROM accounts_group;

-- 가까운 학교를 기록해두기 위한 관계형 테이블
SELECT *
FROM accounts_nearbyschool;

-- 구매 기록 테이블
SELECT *
FROM accounts_paymenthistory;

DESC accounts_paymenthistory;

SELECT COUNT(DISTINCT user_id)
FROM accounts_paymenthistory;

-- 결제 유저가 얼마나 되는지 (전체 결제 유저수)
SELECT
  COUNT(DISTINCT user_id) AS paying_users,
  COUNT(*) AS total_payments
FROM accounts_paymenthistory;

-- 유저당 평균 결제 횟수 
SELECT
  COUNT(*) / COUNT(DISTINCT user_id) AS avg_payments_per_user
FROM accounts_paymenthistory;

-- 결제 월 분포
SELECT
  DATE_FORMAT(created_at, '%Y-%m') AS ym,
  COUNT(*) AS payment_cnt,
  COUNT(DISTINCT user_id) AS paying_users
FROM accounts_paymenthistory
GROUP BY ym
ORDER BY ym;

-- 결제 빈도
SELECT
  payment_cnt,
  COUNT(*) AS user_cnt
FROM (
  SELECT user_id, COUNT(*) AS payment_cnt
  FROM accounts_paymenthistory
  GROUP BY user_id
) t
GROUP BY payment_cnt
ORDER BY payment_cnt;

-- 상품 분석
SELECT
  productId,
  COUNT(*) AS payment_cnt,
  COUNT(DISTINCT user_id) AS users
FROM accounts_paymenthistory
GROUP BY productId
ORDER BY payment_cnt DESC;

-- 날짜 결측 여부 / 없음으로 나옴
SELECT
  COUNT(*) AS total_rows,
  SUM(created_at IS NULL) AS null_created_at
FROM accounts_paymenthistory;

-- 결제 유저 비율(전체 유저 대비)
SELECT
  COUNT(DISTINCT p.user_id) AS paying_users,
  COUNT(DISTINCT u.id) AS total_users,
  ROUND(
    COUNT(DISTINCT p.user_id) / COUNT(DISTINCT u.id),
    4
  ) AS payment_rate
FROM accounts_user u
LEFT JOIN accounts_paymenthistory p
  ON u.id = p.user_id;

-- 유저 컨택 테이블
DESC accounts_user_contacts;

SELECT *
FROM accounts_user_contacts;

-- 포인트 기록 테이블
DESC accounts_pointhistory;

SELECT *
FROM accounts_pointhistory;

-- 포인트 분포
SELECT
  delta_point,
  COUNT(*) AS cnt
FROM accounts_pointhistory
GROUP BY delta_point
ORDER BY delta_point;

-- 포인트 획득 +
SELECT
  delta_point,
  COUNT(*) AS cnt
FROM accounts_pointhistory
WHERE delta_point > 0
GROUP BY delta_point
ORDER BY delta_point;

-- 포인트 사용 -
SELECT
  delta_point,
  COUNT(*) AS cnt
FROM accounts_pointhistory
WHERE delta_point < 0
GROUP BY delta_point
ORDER BY delta_point;

-- 학교 테이블
DESC accounts_school;

SELECT *
FROM accounts_school;

SELECT DISTINCT school_type
FROM accounts_school;

-- 학생수 가장 많은 주소
SELECT address, COUNT(student_count) AS cnt
FROM accounts_school
GROUP BY address
ORDER BY cnt DESC
LIMIT 10;

-- 유저 신고기록 테이블
DESC accounts_timelinereport;

SELECT *
FROM accounts_timelinereport;

SELECT COUNT(id)
FROM accounts_timelinereport;

SELECT DISTINCT reason
FROM accounts_timelinereport;

-- 유저 테이블
DESC accounts_user;

SELECT *
FROM accounts_user;

SELECT COUNT(DISTINCT id)
FROM accounts_user;

SELECT COUNT(id)
FROM accounts_user
WHERE is_superuser = 1;

SELECT gender, COUNT(*)
FROM accounts_user
GROUP BY gender;

SELECT `point`, COUNT(*) AS cnt
FROM accounts_user
GROUP BY `point`
ORDER BY cnt DESC
LIMIT 10;

-- 투표 기록 테이블

SELECT *
FROM accounts_userquestionrecord;

SELECT COUNT(user_id)
FROM accounts_userquestionrecord;

SELECT COUNT(question_piece_id)
FROM accounts_userquestionrecord;

-- 탈퇴 기록 테이블
SELECT *
FROM accounts_userwithdraw;

SELECT DISTINCT reason
FROM accounts_userwithdraw;

-- 포인트 이벤트 참여 테이블
DESC event_receipts;

SELECT *
FROM event_receipts;

SELECT DISTINCT event_id
FROM event_receipts;

SELECT DISTINCT plus_point
FROM event_receipts;

SELECT COUNT(DISTINCT user_id)
FROM event_receipts;

-- 포인트 이벤트 테이블
SELECT *
FROM events;

-- 질문 내용 테이블
SELECT *
FROM polls_question;

SELECT COUNT(DISTINCT id)
FROM polls_question;

SELECT COUNT(DISTINCT question_text)
FROM polls_question;

-- 가장 많이 투표된 질문 top 10
SELECT
  uqr.question_id,
  q.question_text,
  COUNT(*) AS vote_cnt
FROM accounts_userquestionrecord uqr
JOIN polls_question q
  ON uqr.question_id = q.id
GROUP BY uqr.question_id, q.question_text
ORDER BY vote_cnt DESC
LIMIT 10;

-- 가장 많이 열어본 질문 top 10
SELECT
  uqr.question_id,
  q.question_text,
  SUM(uqr.opened_times) AS total_opened_times,
  AVG(uqr.opened_times) AS avg_opened_times,
  COUNT(*) AS record_cnt
FROM accounts_userquestionrecord uqr
JOIN polls_question q
  ON uqr.question_id = q.id
GROUP BY uqr.question_id, q.question_text
ORDER BY total_opened_times DESC
LIMIT 10;

-- 질문 테이블
SELECT *
FROM polls_questionpiece;

SELECT DISTINCT is_voted
FROM polls_questionpiece;

-- 질문에 대한 신고 기록 테이블
SELECT *
FROM polls_questionreport;

SELECT DISTINCT reason
FROM polls_questionreport;

SELECT reason, COUNT(reason) AS cnt
FROM polls_questionreport
GROUP BY reason
ORDER BY cnt DESC;

-- 가장 많이 신고를 받은 질문 내용 확인
SELECT
  qr.question_id,
  q.question_text,
  COUNT(*) AS report_cnt
FROM polls_questionreport qr
JOIN polls_question q
  ON qr.question_id = q.id
GROUP BY qr.question_id, q.question_text
ORDER BY report_cnt DESC
LIMIT 10;

-- 질문 세트 테이블
SELECT *
FROM polls_questionset;

SELECT DISTINCT status
FROM polls_questionset;

-- 질문에 등장하는 유저들 테이블
SELECT *
FROM polls_usercandidate;

SELECT user_id, COUNT(user_id) AS cnt
FROM polls_usercandidate
GROUP BY user_id
ORDER BY cnt DESC
LIMIT 10;




