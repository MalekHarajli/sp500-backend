import os
import psycopg2

def get_db_connection():
    host = os.getenv("SUPABASE_DB_HOST")
    user = os.getenv("SUPABASE_DB_USER")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    dbname = os.getenv("SUPABASE_DB_NAME")
    port = os.getenv("SUPABASE_DB_PORT", "5432")

    print("=== DEBUG: ENV VARS READ BY WORKER ===")
    print("HOST:", host)
    print("USER:", user)
    print("PASSWORD:", password)
    print("DB NAME:", dbname)
    print("PORT:", port)
    print("DATABASE_URL:", os.getenv("DATABASE_URL"))
    print("======================================")

    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        dbname=dbname,
        port=port,
        sslmode="require"
    )

    return conn
