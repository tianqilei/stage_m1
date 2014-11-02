﻿CREATE DATABASE my_data;

CREATE TABLE usr
(
id_usr VARCHAR(10),
name VARCHAR(20),
sex CHAR,
age INT,
CONSTRAINT pk_user PRIMARY KEY (id_usr)
);

CREATE TABLE shop
(
id_shop VARCHAR(10),
name VARCHAR(20),
url VARCHAR(255),
CONSTRAINT pk_shop PRIMARY KEY (id_shop)
);

CREATE TABLE item
(
id_item VARCHAR(10),
name VARCHAR(20),
price FLOAT,
url VARCHAR(255),
description VARCHAR(1023),
CONSTRAINT pk_item PRIMARY KEY (id_item)
);

CREATE TABLE friend
(
id_usr1 VARCHAR(10),
id_usr2 VARCHAR(10),
CONSTRAINT pk_friend PRIMARY KEY (id_usr1,id_usr2),
CONSTRAINT fk_usr_friend1 FOREIGN KEY (id_usr1) REFERENCES usr(id_usr) ON DELETE CASCADE ON UPDATE CASCADE,
CONSTRAINT fk_usr_friend2 FOREIGN KEY (id_usr2) REFERENCES usr(id_usr) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE ordered 
(
id_order VARCHAR(10),
id_usr VARCHAR(10),
id_shop VARCHAR(10),
data_order VARCHAR(50),
total_price FLOAT, 
CONSTRAINT pk_order PRIMARY KEY (id_order),
CONSTRAINT fk_usr_ordered FOREIGN KEY (id_usr) REFERENCES usr(id_usr) ON DELETE CASCADE ON UPDATE CASCADE,
CONSTRAINT fk_shop_ordered FOREIGN KEY (id_shop) REFERENCES shop(id_shop) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE visit
(
id_usr VARCHAR(10),
id_item VARCHAR(10),
date_visit VARCHAR(50),
time_visit TIME,
buy char,
CONSTRAINT pk_visit PRIMARY KEY (id_usr,id_item, date_visit, time_visit),
CONSTRAINT fk_usr_visit FOREIGN KEY (id_usr) REFERENCES usr(id_usr) ON DELETE CASCADE ON UPDATE CASCADE,
CONSTRAINT fk_item_visit FOREIGN KEY (id_item) REFERENCES item(id_item) ON DELETE CASCADE ON UPDATE CASCADE 
);

CREATE TABLE addr 
(
street VARCHAR(50),
city VARCHAR(50),
country VARCHAR (50),
id_usr VARCHAR(10),
CONSTRAINT pk_addr PRIMARY KEY (street, city, country, id_usr),
CONSTRAINT fk_usr_addr FOREIGN KEY (id_usr) REFERENCES usr(id_usr) ON DELETE CASCADE ON UPDATE CASCADE 
);

CREATE TABLE lineitem 
(
id_order VARCHAR(10),
id_item VARCHAR(10),
quantity int,
CONSTRAINT pk_lineitem PRIMARY KEY (id_order,id_item),
CONSTRAINT fk_order_lineitem FOREIGN KEY (id_order) REFERENCES ordered(id_order) ON DELETE CASCADE ON UPDATE CASCADE,
CONSTRAINT fk_item_lineitem FOREIGN KEY (id_item) REFERENCES item(id_item) ON DELETE CASCADE ON UPDATE CASCADE 
);
