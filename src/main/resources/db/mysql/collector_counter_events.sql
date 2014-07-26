CREATE TABLE `metrics_buffer` (
  `id` BIGINT NOT NULL AUTO_INCREMENT,
  `namespace` VARCHAR(32) NOT NULL,
  `metrics` VARCHAR(1024) NOT NULL,
  `timestamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  INDEX `metrics_buffer_namespace_idx` (`namespace`),
  INDEX `metrics_buffer_name_and_time_idx` (`namespace`, `timestamp`))
  ENGINE = INNODB;

CREATE TABLE `metrics_daily` (
  `namespace` VARCHAR(32) NOT NULL,
  `datestamp` DATE NOT NULL,
  `counter_name` varchar(64) NOT NULL,
  `total_count` BIGINT NOT NULL,
  `unique_count` INT NOT NULL,
  `distribution` MEDIUMBLOB NOT NULL,
  PRIMARY KEY (`namespace`, `datestamp`, `counter_name`),
  INDEX `metrics_daily_namespace_idx` (`namespace`),
  INDEX `metrics_daily_name_and_date_idx` (`namespace`, `datestamp`))
  ENGINE = INNODB;
