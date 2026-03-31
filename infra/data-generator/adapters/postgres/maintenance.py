"""PostgreSQL Maintenance - Database updates to trigger CDC."""
from __future__ import annotations

import random
import psycopg2

from config import Config
from services.common import HotCache


class DbMaintenanceAdapter:
    """Handle random DB updates to trigger Debezium CDC."""

    def __init__(self, conn, cfg: Config, cache: HotCache) -> None:
        self.conn = conn
        self.cfg = cfg
        self.cache = cache

    def maybe_update_user_info(self) -> None:
        """Randomly update user information (email, country)."""
        if random.random() >= self.cfg.p_update_user_info:
            return

        if not self.cache.users:
            return

        user_id = random.choice(list(self.cache.users))
        field = random.choice(["email", "country"])

        try:
            with self.conn.cursor() as cur:
                if field == "email":
                    new_email = f"updated_{random.randint(1000, 9999)}@example.com"
                    cur.execute(
                        "UPDATE public.users SET email = %s, updated_at = NOW() WHERE user_id = %s",
                        (new_email, user_id),
                    )
                else:  # country
                    from domain.enums import COUNTRIES
                    new_country = random.choice(COUNTRIES)
                    cur.execute(
                        "UPDATE public.users SET country = %s, updated_at = NOW() WHERE user_id = %s",
                        (new_country, user_id),
                    )
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"[maintenance] ⚠️ User update failed: {e}")
            self.conn.rollback()

    def maybe_update_product_price(self) -> None:
        """Randomly update product prices."""
        if random.random() >= self.cfg.p_update_product_price:
            return

        if not self.cache.products:
            return

        product_id = random.choice(list(self.cache.products))
        factor = random.choice([0.95, 0.97, 1.03, 1.05])  # -5%, -3%, +3%, +5%

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "UPDATE public.products SET price = ROUND(price * %s, 2), updated_at = NOW() WHERE product_id = %s",
                    (factor, product_id),
                )
            self.conn.commit()
        except psycopg2.Error as e:
            print(f"[maintenance] ⚠️ Price update failed: {e}")
            self.conn.rollback()
