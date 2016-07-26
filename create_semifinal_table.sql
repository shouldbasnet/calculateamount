/*-----------------------------------------------------*/
create table if not exists `calculated_details` (
  `uniqueid` varchar(400) not null,
  `rate_type` varchar(255) character set utf8 collate utf8_unicode_ci default null,
  `ceiling` int(11) default null,
  `rate` double(15,2) default null,
  `callduration` double(15,2) default null,
  `hybrid_type` varchar(255) character set utf8 collate utf8_unicode_ci default null,
  `calculated_amount` double(15,2) default '0.00',
  `calculated_at` timestamp null default null,
  `rate_profile_detail_id` int(10) unsigned not null,
  `is_vbr` varchar(3) default null,
  `deleted_at` timestamp null default null
);
/*-----------------------------------------------------*/
create table if not exists `query_log` (
  `callvolumeid` varchar(200) default null,
  `v_query` varchar(2000) default null,
  `execdate` datetime default null
);
/*-----------------------------------------------------*/