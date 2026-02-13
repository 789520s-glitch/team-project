USE final;

SELECT *
FROM accounts_user
LIMIT 10;

-- 사회적 노출 정도 friend_id_list 평균 갯수
SELECT 
    AVG(friend_count) AS avg_friend_count,
    MAX(friend_count) AS max_friend_count,
    MIN(friend_count) AS min_friend_count
FROM (
    SELECT 
        -- 복잡한 수식 대신 JSON 함수 사용 (빈 배열 '[]'도 자동 처리됨)
        JSON_LENGTH(u.friend_id_list) AS friend_count
    FROM accounts_user u
    WHERE u.created_at >= '2023-05-27' 
      AND u.created_at < '2024-05-10'
      -- 투표 기록이 있는 유저만 필터링 (EXISTS 사용으로 가독성 향상)
      AND EXISTS (
          SELECT 1 
          FROM accounts_userquestionrecord r 
          WHERE r.user_id = u.id 
            AND r.created_at >= '2023-05-27' 
            AND r.created_at < '2024-05-10'
      )
) AS sub;

SELECT 
    AVG(sub.cnt) AS avg,
    MAX(sub.cnt) AS max, 
    MIN(sub.cnt) AS min
FROM (
    SELECT DISTINCT u.id, JSON_LENGTH(u.friend_id_list) as cnt
    FROM accounts_user u
    JOIN accounts_userquestionrecord r ON u.id = r.user_id
    WHERE u.created_at >= '2023-05-27' AND u.created_at < '2024-05-10'
      AND r.created_at >= '2023-05-27' AND r.created_at < '2024-05-10'
) sub;

SELECT COUNT(DISTINCT user_id)
FROM polls_usercandidate;

WITH voted_users AS (
    -- 기준: 해당 기간 투표자
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
),
active_senders AS (
    -- 기준: 해당 기간 친구 요청 보낸 사람
    SELECT DISTINCT send_user_id, receive_user_id
    FROM accounts_friendrequest
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
)
SELECT 
    COUNT(v.user_id) AS 전체_투표자_수,
    COUNT(s.send_user_id) AS 친구요청_보낸_투표자_수,
    COUNT(s.receive_user_id) AS 친구요청_받은_투표자_수,
    CONCAT(ROUND(COUNT(s.send_user_id) / COUNT(v.user_id) * 100, 2), '%') AS 친구요청_활동_비율,
	CONCAT(ROUND(COUNT(s.receive_user_id) / COUNT(v.user_id) * 100, 2), '%') AS 친구요청_받는_비율
FROM voted_users v
LEFT JOIN active_senders s ON v.user_id = s.send_user_id
ON v.user_id = s.receive_user_id;

WITH voted_users AS (
    -- 기준: 해당 기간 투표자
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
),
active_senders AS (
    -- 기준: 친구 요청 보낸 사람 (기간 제한 x)
    SELECT DISTINCT send_user_id
    FROM accounts_friendrequest
)
SELECT 
    COUNT(v.user_id) AS 전체_투표자_수,
    COUNT(s.send_user_id) AS 친구요청_보낸_투표자_수,
    CONCAT(ROUND(COUNT(s.send_user_id) / COUNT(v.user_id) * 100, 2), '%') AS 친구요청_활동_비율
FROM voted_users v
LEFT JOIN active_senders s ON v.user_id = s.send_user_id;

WITH voted_users AS (
    -- 기준: 해당 기간 투표자 (중복 제거됨)
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
),
senders AS (
    -- 기준: 해당 기간에 한 번이라도 요청을 '보낸' 사람
    SELECT DISTINCT send_user_id
    FROM accounts_friendrequest
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
),
receivers AS (
    -- 기준: 해당 기간에 한 번이라도 요청을 '받은' 사람
    SELECT DISTINCT receive_user_id
    FROM accounts_friendrequest
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
)
SELECT 
    COUNT(v.user_id) AS 전체_투표자_수,
    
    -- 친구 요청을 보낸 사람 수
    COUNT(s.send_user_id) AS 친구요청_보낸_투표자_수,
    
    -- 친구 요청을 받은 사람 수
    COUNT(r.receive_user_id) AS 친구요청_받은_투표자_수,
    
    -- 비율 계산
    CONCAT(ROUND(COUNT(s.send_user_id) / COUNT(v.user_id) * 100, 2), '%') AS 친구요청_활동_비율, -- (Active)
    CONCAT(ROUND(COUNT(r.receive_user_id) / COUNT(v.user_id) * 100, 2), '%') AS 친구요청_인기_비율    -- (Passive)

FROM voted_users v
-- 1. 보낸 기록이 있는지 확인 (매칭 안 되면 NULL)
LEFT JOIN senders s ON v.user_id = s.send_user_id
-- 2. 받은 기록이 있는지 확인 (매칭 안 되면 NULL)
LEFT JOIN receivers r ON v.user_id = r.receive_user_id;

WITH voted_users AS (
    -- 1. 투표 완료자
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
),
sender_counts AS (
    -- 2. 유저별 친구 요청 보낸 '횟수' 집계 (기간)
    SELECT send_user_id, COUNT(*) AS send_cnt
    FROM accounts_friendrequest
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
    GROUP BY send_user_id
)
SELECT 
    COUNT(v.user_id) AS 전체_투표자_수,
    
    -- NULL(요청 안 보냄)을 0으로 바꿔서 평균
    ROUND(AVG(COALESCE(s.send_cnt, 0)), 2) AS 평균_친구요청_발송수,
    
    -- 최대 / 최소
    MAX(COALESCE(s.send_cnt, 0)) AS 최대_발송수,
	MIN(COALESCE(s.send_cnt, 0)) AS 최소_발송수

FROM voted_users v
LEFT JOIN sender_counts s ON v.user_id = s.send_user_id;

WITH voted_users AS (
    -- 1. 투표 완료자
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09'
),
sender_counts AS (
    -- 2. 유저별 친구 요청 보낸 '횟수' 집계 (누적)
    SELECT send_user_id, COUNT(*) AS send_cnt
    FROM accounts_friendrequest
    GROUP BY send_user_id
)
SELECT 
    COUNT(v.user_id) AS 전체_투표자_수,
    
    -- NULL(요청 안 보냄)을 0으로 바꿔서 평균
    ROUND(AVG(COALESCE(s.send_cnt, 0)), 2) AS 평균_친구요청_발송수,
    
    -- 최대 / 최소
    MAX(COALESCE(s.send_cnt, 0)) AS 최대_발송수,
	MIN(COALESCE(s.send_cnt, 0)) AS 최소_발송수

FROM voted_users v
LEFT JOIN sender_counts s ON v.user_id = s.send_user_id;

SELECT *
FROM accounts_userquestionrecord;

SELECT COUNT(DISTINCT user_id)
FROM accounts_userquestionrecord
WHERE created_at BETWEEN '2023-04-27' AND '2024-05-09';

SELECT COUNT(DISTINCT user_id)
FROM accounts_userquestionrecord
WHERE created_at BETWEEN '2023-05-27' AND '2024-05-09';

SELECT COUNT(DISTINCT user_id)
FROM accounts_userquestionrecord;

SELECT COUNT(DISTINCT id)
FROM accounts_user
WHERE created_at >= '2023-04-28'
AND created_at < '2024-05-09';
-- 가입한 사람 중에서 투표완료한 사람의 비율
SELECT COUNT(DISTINCT u.id) AS 전체_유저수,
	COUNT(DISTINCT r.user_id) AS 투표_완료수,
    ROUND(COUNT(DISTINCT r.user_id) / COUNT(DISTINCT u.id) *100, 2) AS 투표완료_비
FROM accounts_user u
LEFT JOIN accounts_userquestionrecord r 
    ON u.id = r.user_id
WHERE u.created_at >= '2023-04-28'
AND u.created_at < '2024-05-09';

SELECT COUNT(DISTINCT a.user_id) AS 전체_유저수,
	COUNT(DISTINCT r.user_id) AS 투표_완료수,
    ROUND(COUNT(DISTINCT r.user_id) / COUNT(DISTINCT a.user_id) * 100, 2) AS 투표완료_비
FROM accounts_attendance a
LEFT JOIN accounts_userquestionrecord r 
    ON a.user_id = r.user_id
WHERE a.created_at >= '2023-04-28'
AND u.created_at < '2024-05-09';

SELECT
    MIN(att_date) AS min_attendance_date,
    MAX(att_date) AS max_attendance_date
FROM accounts_attendance a
JOIN JSON_TABLE(
    a.attendance_date_list,
    '$[*]' COLUMNS (
        att_date DATE PATH '$'
    )
) jt;

WITH voted_users AS (
    -- 기간 내 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
)
SELECT
    COUNT(DISTINCT q.question_id) AS total_cnt,
    SUM(CASE WHEN q.is_skipped = 1 THEN 1 ELSE 0 END) AS skip_cnt,
    ROUND(
        SUM(CASE WHEN q.is_skipped = 1 THEN 1 ELSE 0 END)
        / COUNT(DISTINCT q.question_id) * 100,
        2
    ) AS skip_rate
FROM polls_questionpiece q
JOIN voted_users v
  ON q.id = v.user_id
WHERE q.created_at >= '2023-04-28'
  AND q.created_at <  '2024-05-09';

SELECT DISTINCT id
FROM polls_questionpiece;

SELECT DISTINCT user_id
FROM accounts_userquestionrecord;

SELECT COUNT(DISTINCT p.user_id)
FROM polls_usercandidate p
JOIN accounts_userquestionrecord a
  ON p.user_id = a.user_id
  WHERE p.created_at >= '2023-04-28'
  AND p.created_at <  '2024-05-09';


WITH voted_users AS (
    -- 기간 내 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
),
usercandidate AS (
	SELECT question_piece_id
	FROM polls_usercandidate
	WHERE created_at >= '2023-04-28'
	AND created_at <  '2024-05-09'
),
questionpiece AS (
	SELECT id
	FROM polls_questionpiece
	WHERE created_at >= '2023-04-28'
	AND created_at <  '2024-05-09'
)
SELECT 

SELECT *
FROM polls_questionpiece;

SELECT 
    SUM(CASE WHEN pqp.is_skipped = 0 THEN 1 ELSE 0 END) AS "0은_몇개",
    SUM(CASE WHEN pqp.is_skipped = 1 THEN 1 ELSE 0 END) AS "1은_몇개"
FROM polls_questionpiece pqp
INNER JOIN accounts_userquestionrecord uqr ON pqp.id = uqr.user_id
WHERE pqp.created_at >= '2023-04-28' 
  AND pqp.created_at < '2024-05-10'
  AND uqr.created_at >= '2023-04-28' 
  AND uqr.created_at < '2024-05-10';
  
WITH voted_users AS (
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
)
SELECT 
    SUM(CASE WHEN pqp.is_skipped = 0 THEN 1 ELSE 0 END) AS skip_0_cnt,
    SUM(CASE WHEN pqp.is_skipped = 1 THEN 1 ELSE 0 END) AS skip_1_cnt
FROM polls_questionpiece q
JOIN voted_users v
  ON q.id = v.user_id
WHERE q.created_at >= '2023-04-28'
  AND q.created_at <  '2024-05-09';
  
SELECT *
FROM polls_questionset;

WITH voted_users AS (
    -- 기간 내 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
),
usercandidate AS (
    -- 투표 완료자가 응답한 question_piece
    SELECT DISTINCT user_id, question_piece_id
    FROM polls_usercandidate
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
)
SELECT
    COUNT(pqp.question_id) AS total_question_cnt,
    SUM(CASE WHEN pqp.is_skipped = 1 THEN 1 ELSE 0 END) AS skipped_question_cnt,
    ROUND(
        SUM(CASE WHEN pqp.is_skipped = 1 THEN 1 ELSE 0 END)
        / COUNT(*) * 100,
        2
    ) AS skip_rate_pct
FROM usercandidate uc
JOIN voted_users vu
  ON uc.user_id = vu.user_id
JOIN polls_questionpiece pqp
  ON uc.question_piece_id = pqp.id
WHERE pqp.created_at >= '2023-04-28'
  AND pqp.created_at <  '2024-05-09';

-- 투표 완료자 중 질문을 한 유저수: 4840명
SELECT COUNT(DISTINCT uqr.user_id) AS common_user_cnt
FROM accounts_userquestionrecord uqr
JOIN polls_usercandidate puc
  ON uqr.user_id = puc.user_id
JOIN polls_questionpiece pqp
  ON puc.question_piece_id = pqp.id
WHERE uqr.created_at >= '2023-04-28'
  AND uqr.created_at <  '2024-05-09'
  AND puc.created_at >= '2023-04-28'
  AND puc.created_at <  '2024-05-09'
  AND pqp.created_at >= '2023-04-28'
  AND pqp.created_at <  '2024-05-09';

SELECT
    uc.user_id,
    COUNT(uc.question_piece_id) AS question_piece_cnt
FROM polls_usercandidate uc
LEFT JOIN accounts_userquestionrecord uq
ON uc.user_id = uq.user_id
WHERE uc.created_at >= '2023-04-28' AND uc.created_at <  '2024-05-09'
AND uq.created_at >= '2023-04-28' AND uq.created_at <  '2024-05-09'
GROUP BY uc.user_id
ORDER BY question_piece_cnt DESC
LIMIT 20;

WITH weekly_votes AS (
    -- 유저 × 주별 투표 횟수
    SELECT
        user_id,
        YEARWEEK(created_at, 3) AS year_week,  -- ISO week (월요일 시작)
        COUNT(*) AS vote_cnt
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28' AND created_at <  '2024-05-09'
    GROUP BY user_id, YEARWEEK(created_at, 3)

),
user_weekly_avg AS (
    -- 유저별 활동 주당 평균 투표 횟수
    SELECT
        user_id,
        SUM(vote_cnt) / COUNT(*) AS avg_votes_per_active_week
    FROM weekly_votes
    GROUP BY user_id
)
SELECT
    ROUND(AVG(avg_votes_per_active_week), 2) AS avg_votes_per_week_per_user
FROM user_weekly_avg;

-- 첫 투표일 -> 최초 결제일
-- accounts_userquestionrecord → min(created_at)
-- accounts_paymenthistory → min(created_at)
WITH first_vote AS (
	SELECT user_id, MIN(created_at) AS first_vote_at
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28' 
    AND created_at <  '2024-05-09'
    GROUP BY user_id
),
first_payment AS (
	SELECT user_id, MIN(created_at) AS first_payment_at
    FROM accounts_paymenthistory
    WHERE created_at >= '2023-04-28' 
    AND created_at <  '2024-05-09'
    GROUP BY user_id
),
vote_to_payment AS (
    SELECT
        v.user_id,
        TIMESTAMPDIFF(
            DAY,
            v.first_vote_at,
            p.first_payment_at
        ) AS diff_days
    FROM first_vote v
    JOIN first_payment p
      ON v.user_id = p.user_id
    WHERE p.first_payment_at > v.first_vote_at
)

SELECT
	COUNT(*) AS user_cnt,
    AVG(diff_days) AS avg_days,
    MIN(diff_days) AS min_days,
    MAX(diff_days) AS max_days
FROM vote_to_payment;

-- 구간 리텐션
-- 투표 완료 유저 중 재방문이 발생한 ‘시점 구간’ 기준 사용자 비율
-- 첫 재방문이 어느 구간에서 발생했는지
WITH voted_users AS (
    -- 유저당 첫 투표
    SELECT
        user_id,
        DATE(MIN(created_at)) AS vote_date
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
    GROUP BY user_id
),
attendance AS (
    SELECT
        user_id,
        CAST(JSON_UNQUOTE(j.value) AS DATE) AS attendance_date
    FROM accounts_attendance,
         JSON_TABLE(attendance_date_list, '$[*]'
           COLUMNS (value VARCHAR(10) PATH '$')
         ) j
),
first_revisit AS (
    SELECT
        v.user_id,
        v.vote_date,
        MIN(a.attendance_date) AS first_revisit_date
    FROM voted_users v
    LEFT JOIN attendance a
      ON v.user_id = a.user_id
     AND a.attendance_date > v.vote_date
    GROUP BY v.user_id, v.vote_date
),
diff AS (
    SELECT
        user_id,
        DATEDIFF(first_revisit_date, vote_date) AS day_diff
    FROM first_revisit
)
SELECT
    COUNT(*) AS total_voted_users,

    COUNT(CASE WHEN day_diff BETWEEN 1 AND 7 THEN 1 END) AS revisit_d7,
    ROUND(COUNT(CASE WHEN day_diff BETWEEN 1 AND 7 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d7_rate,

    COUNT(CASE WHEN day_diff BETWEEN 8 AND 14 THEN 1 END) AS revisit_d14,
    ROUND(COUNT(CASE WHEN day_diff BETWEEN 8 AND 14 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d14_rate,

    COUNT(CASE WHEN day_diff BETWEEN 15 AND 30 THEN 1 END) AS revisit_d30,
    ROUND(COUNT(CASE WHEN day_diff BETWEEN 15 AND 30 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d30_rate,
	
    COUNT(CASE WHEN day_diff > 30 THEN 1 END) AS revisit_d30_plus,
    ROUND(COUNT(CASE WHEN day_diff > 30 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d30_plus_rate
FROM diff;

-- 누적 리텐션
WITH vote AS (
    -- 유저당 첫 투표
    SELECT
        user_id,
        DATE(MIN(created_at)) AS vote_date
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-10'
    GROUP BY user_id
),
attendance AS (
    SELECT
        user_id,
        CAST(JSON_UNQUOTE(j.value) AS DATE) AS attendance_date
    FROM accounts_attendance,
         JSON_TABLE(attendance_date_list, '$[*]'
           COLUMNS (value VARCHAR(10) PATH '$')
         ) j
),
first_revisit AS (
    SELECT
        v.user_id,
        v.vote_date,
        MIN(a.attendance_date) AS first_revisit_date
    FROM vote v
    LEFT JOIN attendance a
      ON v.user_id = a.user_id
     AND a.attendance_date > v.vote_date
    GROUP BY v.user_id, v.vote_date
),
diff AS (
    SELECT
        user_id,
        DATEDIFF(first_revisit_date, vote_date) AS day_diff
    FROM first_revisit
)
SELECT
    COUNT(*) AS total_voted_users,

    COUNT(CASE WHEN day_diff BETWEEN 1 AND 7 THEN 1 END) AS revisit_d7,
    ROUND(COUNT(CASE WHEN day_diff BETWEEN 1 AND 7 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d7_rate,

    COUNT(CASE WHEN day_diff BETWEEN 1 AND 14 THEN 1 END) AS revisit_d14,
    ROUND(COUNT(CASE WHEN day_diff BETWEEN 1 AND 14 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d14_rate,

    COUNT(CASE WHEN day_diff BETWEEN 1 AND 30 THEN 1 END) AS revisit_d30,
    ROUND(COUNT(CASE WHEN day_diff BETWEEN 1 AND 30 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d30_rate,
    
    COUNT(CASE WHEN day_diff > 30 THEN 1 END) AS revisit_d30_plus,
    ROUND(COUNT(CASE WHEN day_diff > 30 THEN 1 END) / COUNT(*) * 100, 2) AS revisit_d30_plus_rate
FROM diff;

WITH vote AS (
    SELECT
        user_id,
        DATE(MIN(created_at)) AS vote_date
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-10'
    GROUP BY user_id
),
attendance AS (
    SELECT
        user_id,
        CAST(JSON_UNQUOTE(j.value) AS DATE) AS attendance_date
    FROM accounts_attendance,
         JSON_TABLE(
             attendance_date_list,
             '$[*]' COLUMNS (value VARCHAR(10) PATH '$')
         ) j
),
revisit AS (
    SELECT DISTINCT
        v.user_id,
        DATEDIFF(a.attendance_date, v.vote_date) AS day_diff
    FROM vote v
    JOIN attendance a
      ON v.user_id = a.user_id
     AND a.attendance_date > v.vote_date
)

SELECT
    COUNT(DISTINCT v.user_id) AS total_voted_users,

    COUNT(DISTINCT CASE WHEN r.day_diff BETWEEN 1 AND 7 THEN v.user_id END) AS revisit_d7,
    ROUND(
        COUNT(DISTINCT CASE WHEN r.day_diff BETWEEN 1 AND 7 THEN v.user_id END)
        / COUNT(DISTINCT v.user_id) * 100,
        2
    ) AS revisit_d7_rate,

    COUNT(DISTINCT CASE WHEN r.day_diff BETWEEN 1 AND 14 THEN v.user_id END) AS revisit_d14,
    ROUND(
        COUNT(DISTINCT CASE WHEN r.day_diff BETWEEN 1 AND 14 THEN v.user_id END)
        / COUNT(DISTINCT v.user_id) * 100,
        2
    ) AS revisit_d14_rate,

    COUNT(DISTINCT CASE WHEN r.day_diff BETWEEN 1 AND 30 THEN v.user_id END) AS revisit_d30,
    ROUND(
        COUNT(DISTINCT CASE WHEN r.day_diff BETWEEN 1 AND 30 THEN v.user_id END)
        / COUNT(DISTINCT v.user_id) * 100,
        2
    ) AS revisit_d30_rate
FROM vote v
LEFT JOIN revisit r
  ON v.user_id = r.user_id;

-- 포인트 이벤트 참여 평균
WITH voted_users AS (
    -- 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-10'
),
event_participation AS (
    -- 이벤트 참여 로그
    SELECT
        user_id,
        COUNT(*) AS event_cnt
    FROM event_receipts
    WHERE created_at >= '2023-04-28'
      AND created_at <  '2024-05-09'
    GROUP BY user_id
)

SELECT
    COUNT(v.user_id) AS total_voted_users,

    COUNT(e.user_id) AS event_participated_users,
    ROUND(
        COUNT(e.user_id) / COUNT(v.user_id) * 100,
        2
    ) AS event_participation_rate,

    ROUND(
        AVG(e.event_cnt),
        2
    ) AS avg_event_cnt_per_participant
FROM voted_users v
LEFT JOIN event_participation e
  ON v.user_id = e.user_id;
  
-- 투표 미경험자와 경험자의 포인트 총합, 빈도수 비교
WITH vote_users AS (
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
),

point_events AS (
    SELECT
        p.user_id,
        p.delta_point
    FROM accounts_pointhistory p
    WHERE p.delta_point > 0
      AND p.created_at BETWEEN '2023-04-28' AND '2024-05-09'
),

labeled AS (
    SELECT
        pe.user_id,
        pe.delta_point,
        CASE
            WHEN vu.user_id IS NOT NULL THEN 'voted'
            ELSE 'not_voted'
        END AS vote_flag
    FROM point_events pe
    LEFT JOIN vote_users vu
      ON pe.user_id = vu.user_id
)

SELECT
    vote_flag,
    COUNT(DISTINCT user_id) AS user_cnt,             -- 포인트 받은 유저 수
    COUNT(*) AS total_events,                         -- 포인트 획득 횟수
    SUM(delta_point) AS total_point,                  -- 총 포인트

    ROUND(COUNT(*) / COUNT(DISTINCT user_id), 2)
        AS avg_events_per_user, -- 1인당 평균 빈도
	  ROUND(SUM(delta_point) / COUNT(DISTINCT user_id), 2)
        AS avg_point_per_user   -- 1인당 평균 포인트 점수
FROM labeled
GROUP BY vote_flag;

-- 첫 포인트 사용까지 걸린 시간
WITH first_attendance AS (
    -- 유저 전체 기준 첫 출석일
    SELECT
        a.user_id,
        MIN(CAST(JSON_UNQUOTE(j.value) AS DATE)) AS first_attendance_date
    FROM accounts_attendance a
    JOIN JSON_TABLE(
        attendance_date_list,
        '$[*]' COLUMNS (value VARCHAR(10) PATH '$')
    ) j
    GROUP BY a.user_id
),

attendance_in_period AS (
    -- 분석 기간 내에 첫 출석한 유저만
    SELECT *
    FROM first_attendance
    WHERE first_attendance_date BETWEEN '2023-04-28' AND '2024-05-09'
),

first_point_use AS (
    -- 유저별 첫 포인트 사용일
    SELECT
        user_id,
        DATE(MIN(created_at)) AS first_point_use_date
    FROM accounts_pointhistory
    WHERE delta_point < 0
    GROUP BY user_id
),

diff AS (
    SELECT
        fa.user_id,
        DATEDIFF(fpu.first_point_use_date, fa.first_attendance_date)
            AS days_to_first_point_use
    FROM attendance_in_period fa
    JOIN first_point_use fpu
      ON fa.user_id = fpu.user_id
    WHERE fpu.first_point_use_date >= fa.first_attendance_date
)

SELECT
    COUNT(*) AS user_cnt,
    ROUND(AVG(days_to_first_point_use), 2) AS avg_days,
    MIN(days_to_first_point_use) AS min_days,
    MAX(days_to_first_point_use) AS max_days
FROM diff;

WITH first_attendance AS (
    -- 유저별 첫 출석일
    SELECT
        a.user_id,
        MIN(CAST(JSON_UNQUOTE(j.value) AS DATETIME)) AS first_attendance_at
    FROM accounts_attendance a
    JOIN JSON_TABLE(
        attendance_date_list,
        '$[*]' COLUMNS (value VARCHAR(20) PATH '$')
    ) j
    GROUP BY a.user_id
),

attendance_in_period AS (
    -- 분석 기간 내 첫 출석 유저
    SELECT *
    FROM first_attendance
    WHERE first_attendance_at BETWEEN '2023-04-28' AND '2024-05-09]'
),

first_point_use AS (
    -- 유저별 첫 포인트 사용 시점
    SELECT
        user_id,
        MIN(created_at) AS first_point_use_at
    FROM accounts_pointhistory
    WHERE delta_point < 0
    GROUP BY user_id
),

joined AS (
    SELECT
        a.user_id,
        a.first_attendance_at,
        f.first_point_use_at,
        TIMESTAMPDIFF(DAY, a.first_attendance_at, f.first_point_use_at)  AS diff_days,
        TIMESTAMPDIFF(HOUR, a.first_attendance_at, f.first_point_use_at) AS diff_hours
    FROM attendance_in_period a
    JOIN first_point_use f
      ON a.user_id = f.user_id
    WHERE f.first_point_use_at >= a.first_attendance_at
)

SELECT
    COUNT(*) AS user_cnt,   -- 첫 출석 이후 포인트 사용 유저 수
    ROUND(
        COUNT(*) / (SELECT COUNT(*) FROM attendance_in_period) * 100, 2
    ) AS user_ratio_pct,
    ROUND(AVG(diff_days), 2) AS avg_days,
    MIN(diff_days) AS min_days,
    MAX(diff_days) AS max_days
FROM joined;

-- 첫 투표일 이후 첫 포인트 사용까지 걸린 시간
WITH first_vote AS (
    -- 유저별 첫 투표 시점
    SELECT
        user_id,
        MIN(created_at) AS first_vote_at
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
    GROUP BY user_id
),

first_point_use AS (
    -- 유저별 첫 포인트 사용 시점
    SELECT
        user_id,
        MIN(created_at) AS first_point_use_at
    FROM accounts_pointhistory
    WHERE delta_point < 0
    AND created_at BETWEEN '2023-04-28' AND '2024-05-09'
    GROUP BY user_id
),

joined AS (
    SELECT
        v.user_id,
        v.first_vote_at,
        p.first_point_use_at,
        DATEDIFF(p.first_point_use_at, v.first_vote_at) AS diff_days
	FROM first_vote v
    JOIN first_point_use p
      ON v.user_id = p.user_id
    WHERE p.first_point_use_at >= v.first_vote_at
)

SELECT
    COUNT(*) AS user_cnt,   -- 첫 투표 이후 포인트 사용 유저 수
    ROUND(
        COUNT(*) / (SELECT COUNT(*) FROM first_vote) * 100, 2
    ) AS user_ratio_pct,
    ROUND(AVG(diff_days), 2) AS avg_days,
    MIN(diff_days) AS min_days,
    MAX(diff_days) AS max_days
FROM joined;

-- 질문에 등장한 횟수(polls_usercandidate) user_id 평균 갯수
WITH voted_users AS (
    -- 분석 기간 내 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
),

usercandidate_cnt AS (
    -- 유저별 질문(후보) 등장 횟수
    SELECT
        user_id,
        COUNT(*) AS candidate_cnt
    FROM polls_usercandidate
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
    GROUP BY user_id
)

SELECT
    COUNT(v.user_id) AS voted_user_cnt,                 -- 투표 유저 수
    ROUND(AVG(IFNULL(u.candidate_cnt, 0)), 2)
        AS avg_candidate_cnt_per_user                  -- 유저당 평균 등장 횟수
FROM voted_users v
LEFT JOIN usercandidate_cnt u
  ON v.user_id = u.user_id;
  
-- 가입일 → 첫 투표까지 평균 소요 시간
WITH first_vote AS (
    -- 분석 기간 내 투표 완료 유저
    SELECT 
		DISTINCT user_id, 
		MIN(created_at) AS first_vote_at
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
    GROUP BY user_id
),
sign_up AS (
	SELECT DISTINCT id, created_at AS sign_up_at
    FROM accounts_user
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
),
joined AS (
	SELECT
	s.id,
	TIMESTAMPDIFF(DAY, s.sign_up_at, v.first_vote_at) AS diff_days
    FROM sign_up s
    JOIN first_vote v
	ON s.id = v.user_id
    WHERE v.first_vote_at >= s.sign_up_at
)
SELECT
    COUNT(*) AS user_cnt,                -- 가입 후 투표까지 간 유저 수
    ROUND(AVG(diff_days), 2) AS avg_days,
    MIN(diff_days) AS min_days,
    MAX(diff_days) AS max_days
FROM joined;

-- 투표 완료자들이 가장 많이 투표한 질문 top10
SELECT q.id, q.question_text, COUNT(q.id) AS question_cnt
FROM polls_question q
JOIN accounts_userquestionrecord r
ON r.question_id = q.id
WHERE q.created_at BETWEEN '2023-04-28' AND '2024-05-09'
AND r.created_at BETWEEN '2023-04-28' AND '2024-05-09'
GROUP BY q.id
ORDER BY question_cnt DESC
LIMIT 10;

SELECT status, COUNT(status)
FROM accounts_userquestionrecord
GROUP BY status;
USE final;
SELECT
    uqr.question_id,
    pq.question_text,
    COUNT(*) AS vote_cnt
FROM accounts_userquestionrecord uqr
JOIN polls_question pq
  ON pq.id = uqr.question_id
WHERE uqr.status = 'C'   -- 투표 완료
  AND uqr.created_at >= '2023-04-28'
  AND uqr.created_at <  '2024-05-09'
  AND pq.created_at >= '2023-04-28'
  AND pq.created_at <  '2024-05-09'
GROUP BY uqr.question_id, pq.question_text
ORDER BY vote_cnt DESC
LIMIT 10;

SELECT *
FROM accounts_userwithdraw;

SELECT u.ban_status, COUNT(DISTINCT u.id) AS ban_cnt
FROM accounts_user u
JOIN accounts_userquestionrecord qr
ON u.id = qr.user_id
WHERE u.created_at >= '2023-04-28'
  AND u.created_at <  '2024-05-09'
  AND qr.created_at >= '2023-04-28'
  AND qr.created_at <  '2024-05-09'
GROUP BY u.ban_status;

SELECT
    u.ban_status,
    COUNT(*) AS total_votes,  -- 전체 투표 수
    SUM(CASE WHEN r.status = 'C' THEN 1 ELSE 0 END) AS completed_votes,
    ROUND(
        SUM(CASE WHEN r.status = 'C' THEN 1 ELSE 0 END)
        / COUNT(*) * 100,
        2
    ) AS completion_rate_pct
FROM accounts_userquestionrecord r
JOIN accounts_user u
  ON r.user_id = u.id
WHERE r.created_at >= '2023-04-28'
  AND r.created_at <  '2024-05-09'
  AND u.created_at >= '2023-04-28'
  AND u.created_at <  '2024-05-09'
GROUP BY u.ban_status
ORDER BY completion_rate_pct DESC;

SELECT
    u.ban_status,
    COUNT(*) AS user_cnt
FROM accounts_user u
JOIN accounts_userquestionrecord r
  ON u.id = r.user_id
WHERE r.user_id IS NULL
  AND u.created_at BETWEEN '2023-04-28' AND '2024-05-09'
  AND r.created_at >= '2023-04-28' AND r.created_at < '2024-05-09'
GROUP BY u.ban_status
ORDER BY user_cnt DESC;

-- 1. 학급에 4명이상 학교 학생수 40명이상이여야함
-- 55,256 / 68.29%만 투표 가능 → 투표 완료자 %

-- 학생수 40명 이상인 학교수: 3897개
SELECT COUNT(id)
FROM accounts_school
WHERE student_count >= 40;

-- 학급에 학생 수가 4명 이상인 학급
SELECT group_id, COUNT(DISTINCT id)
FROM accounts_user
GROUP BY group_id
HAVING COUNT(DISTINCT id) >= 4;
-- 56960개
SELECT COUNT(*) AS class_cnt
FROM (
    SELECT group_id
    FROM accounts_user
    GROUP BY group_id
    HAVING COUNT(DISTINCT id) >= 4
) t;

SELECT *
FROM accounts_group;

SELECT COUNT(u.group_id) AS cnt
FROM accounts_user u
JOIN accounts_school s
ON u.group_id = s.id
WHERE s.student_count >= 40;
-- 학생수가 40명 이상 + 학생수가 4명 이상인 학급
WITH school_40 AS (
    -- 학생 수 40명 이상인 학교
    SELECT id AS school_id
    FROM accounts_school
    WHERE student_count >= 40
),
class_4 AS (
    -- 학생 수 4명 이상인 학급
    SELECT
        group_id
    FROM accounts_user
    GROUP BY group_id
    HAVING COUNT(DISTINCT id) >= 4
)
SELECT
    COUNT(DISTINCT u.id) AS student_cnt,
    COUNT(DISTINCT u.group_id) AS group_cnt
FROM accounts_user u
JOIN accounts_group g
  ON u.group_id = g.id
JOIN school_40 s
  ON g.school_id = s.school_id
JOIN class_4 c
  ON u.group_id = c.group_id;
-- 전체 학생수 / 학급수
SELECT COUNT(DISTINCT id) AS total_cnt,
COUNT(DISTINCT group_id)
FROM accounts_user;
-- 투표 가능한 유저 
WITH school_40 AS (
    -- 학생 수 40명 이상인 학교
    SELECT id AS school_id
    FROM accounts_school
    WHERE student_count >= 40
),
class_4 AS (
    -- 학생 수 4명 이상인 학급
    SELECT
        group_id
    FROM accounts_user
    GROUP BY group_id
    HAVING COUNT(DISTINCT id) >= 4
),
eligible_students AS (
    -- 조건을 만족하는 학생 (분모)
    SELECT DISTINCT
        u.id AS user_id
    FROM accounts_user u
    JOIN accounts_group g
      ON u.group_id = g.id
    JOIN school_40 s
      ON g.school_id = s.school_id
    JOIN class_4 c
      ON u.group_id = c.group_id
),
voted_user AS (
    -- 분석 기간 내 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
)
SELECT
    COUNT(DISTINCT e.user_id) AS eligible_student_cnt,   -- 조건 만족 학생 수
    COUNT(DISTINCT v.user_id) AS voted_student_cnt,      -- 그중 투표 완료자 수
    ROUND(
        COUNT(DISTINCT v.user_id) * 100.0
        / COUNT(DISTINCT e.user_id),
        2
    ) AS voted_ratio                                     -- 투표 완료 비율
FROM eligible_students e
LEFT JOIN voted_user v
  ON e.user_id = v.user_id;

-- 결제 완료자 중 투표 완료자의 비율
WITH voted_user AS (
    -- 투표 완료 유저
    SELECT DISTINCT user_id
    FROM accounts_userquestionrecord
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
),
payed_user AS (
	SELECT DISTINCT user_id
    FROM accounts_paymenthistory
    WHERE created_at BETWEEN '2023-04-28' AND '2024-05-09'
)
SELECT
    COUNT(DISTINCT p.user_id) AS payed_user_cnt,        -- 결제 완료자 수 (분모)
    COUNT(DISTINCT v.user_id) AS voted_user_cnt,        -- 결제자 중 투표 완료자 수 (분자)
    ROUND(
        COUNT(DISTINCT v.user_id) * 100.0
        / COUNT(DISTINCT p.user_id),
        2
    ) AS voted_ratio  
FROM payed_user p
LEFT JOIN voted_user v
ON p.user_id = v.user_id;

SELECT user_id, COUNT(created_at)
FROM accounts_paymenthistory
GROUP BY user_id
ORDER BY COUNT(created_at) DESC;

SELECT *
FROM hackle_properties
WHERE user_id = '1527451';

SELECT *
FROM hackle_events
WHERE session_id ='f0a624d3-95d8-46ff-968b-f0d303043427';

WITH voted_user AS (
    -- 투표 완료 유저 (투표 시점 포함)
    SELECT
        user_id
	FROM accounts_userquestionrecord
    GROUP BY user_id
),
payed_user AS (
    -- 결제 완료 유저 (결제 시점 포함)
    SELECT
        user_id
	FROM accounts_paymenthistory
    GROUP BY user_id
)
SELECT
    COUNT(p.user_id) AS total_payed_user,
    COUNT(v.user_id) AS total_voted_user
FROM payed_user p
LEFT JOIN voted_user v
  ON p.user_id = v.user_id;

SELECT
  productId,
  COUNT(DISTINCT p.user_id) AS payed_cnt,
  COUNT(DISTINCT v.user_id) AS voted_cnt,
  ROUND(COUNT(DISTINCT v.user_id)*100.0 / COUNT(DISTINCT p.user_id), 2) AS vote_rate
FROM accounts_paymenthistory p
LEFT JOIN accounts_userquestionrecord v
  ON p.user_id = v.user_id
WHERE p.created_at BETWEEN '2023-04-28' AND '2024-05-09'
GROUP BY productId;

SELECT MIN(u.created_at), MAX(u.created_at) 
FROM accounts_user u
JOIN accounts_userquestionrecord r
ON u.id = r.user_id;

