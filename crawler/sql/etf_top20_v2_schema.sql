-- ============================================================
-- ETF Top 20 v2 - MySQL Schema
-- 資料源: 證交所 T86 + yfinance (免費, 不需 token)
-- ============================================================

USE mydb;

CREATE TABLE IF NOT EXISTS `EtfTop20BuyInstitutionalV2` (
    `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `date` DATE NOT NULL,
    `stock_id` VARCHAR(10) NOT NULL,
    `stock_name` VARCHAR(50),
    `open_price` FLOAT,
    `close_price` FLOAT,
    `trading_volume_shares` BIGINT COMMENT '成交股數',
    `trading_value` BIGINT COMMENT '成交金額 (元, 近似)',
    `five_day_trend_pct` FLOAT COMMENT '5 日趨勢 (%)',
    `rank_num` INT COMMENT '三大法人淨買超名次',
    `foreign_buy` BIGINT COMMENT '外資買進股數',
    `foreign_sell` BIGINT COMMENT '外資賣出股數',
    `foreign_net` BIGINT COMMENT '外資買賣超股數',
    `trust_buy` BIGINT COMMENT '投信買進股數',
    `trust_sell` BIGINT COMMENT '投信賣出股數',
    `trust_net` BIGINT COMMENT '投信買賣超股數',
    `dealer_buy` BIGINT COMMENT '自營商買進股數',
    `dealer_sell` BIGINT COMMENT '自營商賣出股數',
    `dealer_net` BIGINT COMMENT '自營商買賣超股數',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY `uk_date_stock` (`date`, `stock_id`),
    KEY `idx_date` (`date`),
    KEY `idx_rank` (`date`, `rank_num`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- Redash 查詢範例：

-- 1. 取最新一日前 20 名
SELECT rank_num, stock_id, stock_name, open_price, close_price,
       trading_volume_shares, trading_value, five_day_trend_pct
FROM EtfTop20BuyInstitutionalV2
WHERE date = (SELECT MAX(date) FROM EtfTop20BuyInstitutionalV2)
ORDER BY rank_num;

-- 2. 過去 30 天最常進入 Top 20 的 ETF
SELECT stock_id, stock_name, COUNT(*) AS days_in_top20, AVG(rank_num) AS avg_rank
FROM EtfTop20BuyInstitutionalV2
WHERE date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY stock_id, stock_name
ORDER BY days_in_top20 DESC, avg_rank ASC
LIMIT 20;