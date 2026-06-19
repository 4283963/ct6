-- ============================================================================
-- task_dispatcher 核心表结构
-- 字符集 utf8mb4 以支持完整 Unicode（含 emoji / 中文 webhook payload）
-- ============================================================================

CREATE TABLE IF NOT EXISTS `tasks` (
  `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `task_key`     VARCHAR(128)    NOT NULL COMMENT '业务幂等键，全局唯一',
  `webhook_url`  VARCHAR(512)    NOT NULL,
  `method`       VARCHAR(16)     NOT NULL DEFAULT 'POST',
  `headers`      JSON            NULL,
  `payload`      LONGTEXT        NULL,
  `max_retries`  INT             NOT NULL DEFAULT 5,
  `attempt`      INT             NOT NULL DEFAULT 0,
  `state`        VARCHAR(16)     NOT NULL DEFAULT 'pending',
  `next_run_at`  DATETIME(3)     NOT NULL,
  `priority`     INT             NOT NULL DEFAULT 0,
  `created_at`   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at`   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_task_key` (`task_key`),
  KEY `idx_dispatch` (`state`, `priority` DESC, `next_run_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='调度任务表';

CREATE TABLE IF NOT EXISTS `task_executions` (
  `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `task_key`       VARCHAR(128)    NOT NULL,
  `attempt`        INT             NOT NULL,
  `delivery_token` VARCHAR(64)     NOT NULL COMMENT '投递幂等令牌 task_key#attempt',
  `status_code`    INT             NOT NULL DEFAULT 0,
  `response_body`  TEXT            NULL,
  `error_message`  TEXT            NULL,
  `duration_ms`    BIGINT          NOT NULL DEFAULT 0,
  `instance_id`    VARCHAR(64)     NOT NULL DEFAULT '',
  `created_at`     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_delivery_token` (`delivery_token`),
  KEY `idx_task_exec` (`task_key`, `attempt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务投递执行记录表';
