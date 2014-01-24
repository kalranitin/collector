CREATE TABLE `metrics_subscription` (
  `id` INT NULL AUTO_INCREMENT,
  `identifier` VARCHAR(32) NULL,
  `distribution_for` VARCHAR(256) NULL,
  PRIMARY KEY (`id`),
  INDEX `subscription_identifier_indx` (`identifier` ASC))
  ENGINE = INNODB;

CREATE TABLE `metrics_daily` (
  `id` BIGINT NULL AUTO_INCREMENT,
  `subscription_id` INT NOT NULL,
  `metrics` VARCHAR(512) NULL,
  `created_date` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `metrics_day_subscription_id_idx` (`subscription_id` ASC),
  INDEX `metrics_day_dt_indx` (`created_date` ASC))
  ENGINE = INNODB;

CREATE TABLE `metrics_daily_roll_up` (
  `id` VARCHAR(32) NOT NULL,
  `subscription_id` INT NOT NULL,
  `metrics` MEDIUMBLOB NOT NULL,
  `created_date` DATE NOT NULL,
  PRIMARY KEY (`id`),
  INDEX `metrics_day_roll_subscription_id_idx` (`subscription_id` ASC),
  INDEX `metrics_day_roll_dt_indx` (`created_date` ASC))
  ENGINE = INNODB;