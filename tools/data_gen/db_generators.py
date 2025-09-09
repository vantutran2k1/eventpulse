import time
from abc import ABC
from typing import override

import psycopg2
from faker import Faker

from tools.data_gen.base_generator import BaseGenerator

fake = Faker()


class DbRecordGenerator(BaseGenerator, ABC):
    def __init__(self, conn_params, **kwargs):
        super().__init__(**kwargs)
        self.conn = psycopg2.connect(**conn_params)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor()

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def _create_table_if_not_exists(self):
        raise NotImplementedError("Must be implemented by subclass")

    @override
    def run(self):
        self._create_table_if_not_exists()
        super().run()


class OrderGenerator(DbRecordGenerator):
    @override
    def _create_table_if_not_exists(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "id UUID PRIMARY KEY, "
            "user_id UUID NOT NULL, "
            "product_id UUID NOT NULL, "
            "quantity INT NOT NULL, "
            "price NUMERIC(10, 2) NOT NULL, "
            "status VARCHAR(256) NOT NULL, "
            "created_at BIGINT NOT NULL, "
            "updated_at BIGINT NOT NULL"
            ");"
        )
        self.conn.commit()

    def generate_one(self):
        order_id = fake.uuid4()
        user_id = fake.uuid4()
        product_id = fake.uuid4()
        quantity = fake.pyint(min_value=1, max_value=100)
        price = round(fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=10, max_value=500), 2)
        status = fake.random_element(["CREATED", "CONFIRMED", "CANCELLED"])
        now = int(time.time() * 1000)

        self.cursor.execute(
            "INSERT INTO orders (id, user_id, product_id, quantity, price, status, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;",
            (order_id, user_id, product_id, quantity, price, status, now, now)
        )
        db_id = self.cursor.fetchone()[0]
        print(f"[Postgres] Order inserted ID={db_id}, ProductID={product_id}, Status={status}")


class PaymentGenerator(DbRecordGenerator):
    @override
    def _create_table_if_not_exists(self):
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS payments ("
            "id UUID PRIMARY KEY, "
            "order_id UUID NOT NULL, "
            "user_id UUID NOT NULL, "
            "amount NUMERIC(10, 2) NOT NULL, "
            "status VARCHAR(256) NOT NULL, "
            "created_at BIGINT NOT NULL, "
            "updated_at BIGINT NOT NULL"
            ");"
        )
        self.conn.commit()

    def generate_one(self):
        payment_id = fake.uuid4()
        order_id = fake.uuid4()
        user_id = fake.uuid4()
        amount = round(fake.pyfloat(left_digits=10, right_digits=2, positive=True, min_value=1, max_value=1000), 2)
        status = fake.random_element(["PAID", "FAILED"])
        now = int(time.time() * 1000)

        self.cursor.execute(
            "INSERT INTO payments (id, order_id, user_id, amount, status, created_at, updated_at) "
            "VALUES (%s, %s, %s, %s,  %s, %s, %s) RETURNING id;",
            (payment_id, order_id, user_id, amount, status, now, now)
        )
        print(f"[Postgres] Payment inserted ID={order_id}, Amount={amount}, Status={status}")
