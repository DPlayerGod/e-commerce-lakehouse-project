"""PostgreSQL Seed - Create reference data."""
from __future__ import annotations

import random
from config import Config
from services.common import HotCache, rid
from domain.enums import COUNTRIES


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
    
    print(f"[fakegen] Created {cfg.seed_products} products")
    conn.commit()
    cur.close()
