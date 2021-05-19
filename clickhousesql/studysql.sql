show databases ;

use default;

create table t_order_mt(
    id UInt32,
    sku_id String,
    total_amount Decimal(16,2),
    create_time  Datetime
 ) engine =MergeTree
   partition by toYYYYMMDD(create_time)
   primary key (id)
   order by (id,sku_id);

insert into  t_order_mt values
(101,'sku_001',1000.00,'2020-06-01 12:00:00') ,
(102,'sku_002',2000.00,'2020-06-01 11:00:00'),
(102,'sku_004',2500.00,'2020-06-01 12:00:00'),
(102,'sku_002',2000.00,'2020-06-01 13:00:00'),
(102,'sku_002',12000.00,'2020-06-01 13:00:00'),
(102,'sku_002',600.00,'2020-06-02 12:00:00');


select * from t_order_mt;

create table t_order_rmt(
    id UInt32,
    sku_id String,
    total_amount Decimal(16,2) ,
    create_time  Datetime
 ) engine =ReplacingMergeTree(create_time)
   partition by toYYYYMMDD(create_time)
   primary key (id)
   order by (id, sku_id);


insert into  t_order_rmt values
(101,'sku_001',1000.00,'2020-06-01 12:00:00') ,
(102,'sku_002',2000.00,'2020-06-01 11:00:00'),
(102,'sku_004',2500.00,'2020-06-01 12:00:00'),
(102,'sku_002',2000.00,'2020-06-01 13:00:00'),
(102,'sku_002',12000.00,'2020-06-01 13:00:00'),
(102,'sku_002',600.00,'2020-06-02 12:00:00');

select * from t_order_rmt;

OPTIMIZE TABLE t_order_rmt FINAL;

create table t_order_smt(
    id UInt32,
    sku_id String,
    total_amount Decimal(16,2) ,
    create_time  Datetime
 ) engine =SummingMergeTree(total_amount)
   partition by toYYYYMMDD(create_time)
   primary key (id)
   order by (id,sku_id );

insert into  t_order_smt values
(101,'sku_001',1000.00,'2020-06-01 12:00:00'),
(102,'sku_002',2000.00,'2020-06-01 11:00:00'),
(102,'sku_004',2500.00,'2020-06-01 12:00:00'),
(102,'sku_002',2000.00,'2020-06-01 13:00:00'),
(102,'sku_002',12000.00,'2020-06-01 13:00:00'),
(102,'sku_002',600.00,'2020-06-02 12:00:00');

select * from t_order_smt;

OPTIMIZE TABLE t_order_smt FINAL;


alter table t_order_mt delete where 1=1;

select * from t_order_mt;

insert into  t_order_mt values
(101,'sku_001',1000.00,'2020-06-01 12:00:00'),
(102,'sku_002',2000.00,'2020-06-01 12:00:00'),
(103,'sku_004',2500.00,'2020-06-01 12:00:00'),
(104,'sku_002',2000.00,'2020-06-01 12:00:00'),
(105,'sku_003',600.00,'2020-06-02 12:00:00'),
(106,'sku_001',1000.00,'2020-06-04 12:00:00'),
(107,'sku_002',2000.00,'2020-06-04 12:00:00'),
(108,'sku_004',2500.00,'2020-06-04 12:00:00'),
(109,'sku_002',2000.00,'2020-06-04 12:00:00'),
(110,'sku_003',600.00,'2020-06-01 12:00:00');

select id , sku_id,sum(total_amount) from  t_order_mt group by id,sku_id with rollup;

select id , sku_id,sum(total_amount) from  t_order_mt group by id,sku_id with cube;

select id , sku_id,sum(total_amount) from  t_order_mt group by id,sku_id with totals;

create table t_order_rep (
    id UInt32,
    sku_id String,
    total_amount Decimal(16,2),
    create_time  Datetime
 ) engine =ReplicatedMergeTree('/clickhouse/tables/01/t_order_rep','rep_202')
   partition by toYYYYMMDD(create_time)
   primary key (id)
   order by (id,sku_id);

insert into t_order_rep values
(101,'sku_001',1000.00,'2020-06-01 12:00:00'),
(102,'sku_002',2000.00,'2020-06-01 12:00:00'),
(103,'sku_004',2500.00,'2020-06-01 12:00:00'),
(104,'sku_002',2000.00,'2020-06-01 12:00:00'),
(105,'sku_003',600.00,'2020-06-02 12:00:00');

create table st_order_mt on cluster gmall_cluster (
    id UInt32,
    sku_id String,
    total_amount Decimal(16,2),
    create_time  Datetime
 ) engine =ReplicatedMergeTree('/clickhouse/tables/{shard}/st_order_mt_0105','{replica}')
   partition by toYYYYMMDD(create_time)
   primary key (id)
   order by (id,sku_id);


create table st_order_mt_all on cluster gmall_cluster
(
    id UInt32,
    sku_id String,
    total_amount Decimal(16,2),
    create_time  Datetime
)engine = Distributed(gmall_cluster,default, st_order_mt,hiveHash(sku_id));


insert into  st_order_mt_all values
(201,'sku_001',1000.00,'2020-06-01 12:00:00') ,
(202,'sku_002',2000.00,'2020-06-01 12:00:00'),
(203,'sku_004',2500.00,'2020-06-01 12:00:00'),
(204,'sku_002',2000.00,'2020-06-01 12:00:00'),
(205,'sku_003',600.00,'2020-06-02 12:00:00');

SELECT *  FROM st_order_mt_all;


