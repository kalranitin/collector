
create table feed_events
(
    offset INTEGER PRIMARY KEY AUTO_INCREMENT,
    created_at bigint,
    channel varchar(256),
    metadata mediumtext,
    event mediumtext,
    subscription_id integer
);

create table subscriptions
(
  id INTEGER PRIMARY KEY AUTO_INCREMENT,
  topic varchar(128),
  metadata varchar(512),
  channel varchar(128)
);

CREATE TABLE feeds (
  feed_key VARCHAR(50) NOT NULL,
  feed MEDIUMBLOB NOT NULL,
  PRIMARY KEY (feed_key)
);

create index subscriptions_topic_idx on subscriptions (topic);
