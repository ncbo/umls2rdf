#!/usr/bin/env python3

import argparse

import pymysql

import conf


def connect_server():
    return pymysql.connect(
        host=conf.DB_HOST,
        user=conf.DB_USER,
        passwd=conf.DB_PASS,
        charset="utf8mb4",
        autocommit=True,
    )


def database_exists(connection, db_name):
    with connection.cursor() as cursor:
        cursor.execute("SHOW DATABASES LIKE %s", (db_name,))
        return cursor.fetchone() is not None


def create_database(connection, db_name):
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE DATABASE IF NOT EXISTS `{db_name}`
            CHARACTER SET utf8mb4
            COLLATE utf8mb4_unicode_ci
            """.format(db_name=db_name)
        )
        cursor.execute(
            "ALTER DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci".format(
                db_name=db_name
            )
        )


def drop_database(connection, db_name):
    with connection.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS `{db_name}`".format(db_name=db_name))


def ensure_database(db_name, recreate=False):
    connection = connect_server()
    try:
        if recreate:
            drop_database(connection, db_name)
        create_database(connection, db_name)
    finally:
        connection.close()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Drop the configured database before creating it.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    ensure_database(conf.DB_NAME, recreate=args.recreate)
    print(conf.DB_NAME)


if __name__ == "__main__":
    main()
