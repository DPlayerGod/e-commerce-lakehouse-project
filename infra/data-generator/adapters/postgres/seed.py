"""PostgreSQL Seed - Create reference data."""
from __future__ import annotations

import random
import time
from config import Config
from services.common import HotCache, rid
from domain.enums import COUNTRIES


def set_replica_identity(conn, table_name: str, timeout_s: int = 10) -> None:
    """Set REPLICA IDENTITY FULL with retry logic."""
    cur = conn.cursor()
    start = time.monotonic()
    
    while True:
        try:
            # Set REPLICA IDENTITY FULL
            cur.execute(f"ALTER TABLE public.{table_name} REPLICA IDENTITY FULL")
            conn.commit()
            
            # Verify
            cur.execute(f"SELECT relname, relreplident FROM pg_class WHERE relname='{table_name}'")
            result = cur.fetchone()
            replica_ident = result[1] if result else '?'
            
            if replica_ident == 'f':
                print(f"[fakegen] ✅ {table_name} REPLICA IDENTITY set to FULL")
                break
            else:
                print(f"[fakegen] ⚠️  {table_name} REPLICA IDENTITY = {replica_ident}, retrying...")
                if time.monotonic() - start > timeout_s:
                    raise Exception(f"Timeout setting REPLICA IDENTITY for {table_name}")
                time.sleep(0.5)
        except Exception as e:
            print(f"[fakegen] ❌ Error setting REPLICA IDENTITY for {table_name}: {e}")
            raise
    
    cur.close()


def create_users_table(conn) -> None:
    """Create users table."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.users (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100),
            country VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    
    # Set REPLICA IDENTITY FULL (with retry)
    set_replica_identity(conn, "users")


def create_products_table(conn) -> None:
    """Create products table."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.products (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(50) UNIQUE NOT NULL,
            title VARCHAR(255),
            category VARCHAR(100),
            price DECIMAL(10, 2),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    cur.close()
    
    # Set REPLICA IDENTITY FULL (with retry)
    set_replica_identity(conn, "products")


def seed_postgres(conn, cfg: Config, cache: HotCache) -> None:
    """Seed PostgreSQL with reference data."""
    cur = conn.cursor()
    
    print("[fakegen] Creating tables...")
    create_users_table(conn)
    create_products_table(conn)
    
    # Clear existing data
    cur.execute("DELETE FROM public.users")
    cur.execute("DELETE FROM public.products")
    
    print("[fakegen] Seeding users...")
    for i in range(cfg.seed_users):
        user_id = rid("usr", 8)
        email = f"user_{i}@example.com"
        country = random.choice(COUNTRIES)
        
        cur.execute(
            "INSERT INTO public.users (user_id, email, country) VALUES (%s, %s, %s)",
            (user_id, email, country)
        )
        cache.users.append(user_id)
    
    print(f"[fakegen] Created {cfg.seed_users} users")
    
    print("[fakegen] Seeding products...")
    categories = ["Electronics", "Home", "Sports", "Fashion", "Books", "Beauty"]
    for i in range(cfg.seed_products):
        product_id = rid("prd", 8)
        title = f"Product {i}"
        category = random.choice(categories)
        price = round(random.uniform(10.0, 500.0), 2)
        
        cur.execute(
            "INSERT INTO public.products (product_id, title, category, price) VALUES (%s, %s, %s, %s)",
            (product_id, title, category, price)
        )
        cache.products.append(product_id)
        cache.product_prices[product_id] = price  # Store price for order generation
    
    print(f"[fakegen] Created {cfg.seed_products} products")
    conn.commit()
    
    # Verify and ensure REPLICA IDENTITY FULL
    print("[fakegen] Verifying REPLICA IDENTITY...")
    for table_name in ["users", "products"]:
        cur.execute(f"SELECT relname, relreplident FROM pg_class WHERE relname='{table_name}'")
        result = cur.fetchone()
        if result:
            replica_ident = result[1]
            if replica_ident == 'f':
                print(f"[fakegen] ✅ {table_name} REPLICA IDENTITY = FULL")
            else:
                print(f"[fakegen] ⚠️  {table_name} REPLICA IDENTITY = {replica_ident}, fixing...")
                cur.execute(f"ALTER TABLE public.{table_name} REPLICA IDENTITY FULL")
                conn.commit()
                cur.execute(f"SELECT relname, relreplident FROM pg_class WHERE relname='{table_name}'")
                new_result = cur.fetchone()
                print(f"[fakegen] ✅ {table_name} REPLICA IDENTITY fixed to FULL")
    
    cur.close()
