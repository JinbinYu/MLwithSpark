-- DataStudio database for meta data management

DROP DATABASE IF EXISTS datastudio;
CREATE DATABASE IF NOT EXISTS datastudio;
USE datastudio;

DROP TABLE IF EXISTS user, model, dataset;

CREATE TABLE user (
	userId INT(10) NOT NULL PRIMARY KEY AUTO_INCREMENT,
	userName VARCHAR(20) NOT NULL unique,
	password VARCHAR(20) NOT NULL
)AUTO_INCREMENT=1;

CREATE TABLE dataset (
	userId INT(10) NOT NULL ,
	datasetName VARCHAR(20) NOT NULL,
	dataType VARCHAR(20) NOT NULL,
	dataPath VARCHAR(100) NOT NULL,
	createTime VARCHAR(20)
);

CREATE TABLE model (
	userId INT(10) NOT NULL,
	modelName VARCHAR(20) NOT NULL,
	algoName VARCHAR(20) NOT NULL,
	status INT(10) NOT NULL,
	modelPath VARCHAR(50) NOT NULL,
	time VARCHAR(20),
	algoPara VARCHAR(200),
	createTime VARCHAR(20),
	datasetName VARCHAR(20)
)

