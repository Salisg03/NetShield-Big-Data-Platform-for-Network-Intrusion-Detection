-- Fichier: database/init.cql
CREATE KEYSPACE IF NOT EXISTS netshield WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE netshield;
CREATE TABLE IF NOT EXISTS alertes ( "Label" text PRIMARY KEY, count bigint );