-- ============================================================
-- ETF Top 20 三大法人買超 - MySQL Schema
-- ============================================================
-- 此檔案提供給下游團隊 (Redash 視覺化 / Airflow 排程) 參考
-- 程式碼會在第一次執行時自動建表 (CREATE TABLE IF NOT EXISTS),
-- 但若要事先手動建立或了解結構, 請參考本檔。

USE mydb;

CREATE TABLE IF NOT EXISTS `EtfTop20BuyInstitutional` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '流水號',
    `date` DATE NOT NULL COMMENT '交易日期',
    `stock_id` VARCHAR(10) NOT NULL COMMENT 'ETF 代號, 例: 0050',
    `stock_name` VARCHAR(50) COMMENT 'ETF 中文名稱, 例: 元大台灣50',
    `trading_volume` BIGINT COMMENT '當日交易量 (股)',
    `open_price` FLOAT COMMENT '開盤價',
    `close_price` FLOAT COMMENT '收盤價',
    `high_price` FLOAT COMMENT '最高價',
    `low_price` FLOAT COMMENT '最低價',
    `daily_change_pct` FLOAT COMMENT '當日漲跌幅 (%)',
    `five_day_trend_pct` FLOAT COMMENT '5 日趨勢 (近 5 個交易日漲跌幅 %)',
    `institutional_net_buy` BIGINT COMMENT '三大法人淨買超 (股)',
    `rank_num` INT COMMENT '當日名次 (1-20, 1 = 買超最多)',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '寫入時間',
    UNIQUE KEY `uk_date_stock` (`date`, `stock_id`),
    KEY `idx_date` (`date`),
    KEY `idx_rank` (`date`, `rank_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
COMMENT='ETF 三大法人買超前 20 名每日快照';


-- ============================================================
-- 給 Redash / 視覺化團隊的常用查詢範例
-- ============================================================

-- 1. 取最新一日的前 20 名
SELECT
    rank_num,
    stock_id,
    stock_name,
    close_price,
    daily_change_pct,
    five_day_trend_pct,
    institutional_net_buy,
    trading_volume
FROM EtfTop20BuyInstitutional
WHERE date = (SELECT MAX(date) FROM EtfTop20BuyInstitutional)
ORDER BY rank_num;


-- 2. 看某檔 ETF 最近 30 天的排名變化
SELECT
    date,
    rank_num,
    close_price,
    daily_change_pct,
    institutional_net_buy
FROM EtfTop20BuyInstitutional
WHERE stock_id = '0050'
  AND date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY date DESC;


-- 3. 統計過去 30 天最常進入 Top 20 的 ETF (人氣榜)
SELECT
    stock_id,
    stock_name,
    COUNT(*) AS days_in_top20,
    AVG(rank_num) AS avg_rank,
    SUM(institutional_net_buy) AS total_net_buy
FROM EtfTop20BuyInstitutional
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY stock_id, stock_name
ORDER BY days_in_top20 DESC, avg_rank ASC
LIMIT 20;


-- 4. 5 日趨勢正向且當日上榜的標的 (動能標的)
SELECT
    date,
    rank_num,
    stock_id,
    stock_name,
    daily_change_pct,
    five_day_trend_pct
FROM EtfTop20BuyInstitutional
WHERE date = (SELECT MAX(date) FROM EtfTop20BuyInstitutional)
  AND five_day_trend_pct > 0
ORDER BY five_day_trend_pct DESC;
