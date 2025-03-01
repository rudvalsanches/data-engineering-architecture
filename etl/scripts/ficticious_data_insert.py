import os
import psycopg2
from faker import Faker
import random

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = os.environ.get("DB_PORT", 5432)
DB_USER = os.environ.get("DB_USER", "user")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "password")
DB_NAME = os.environ.get("DB_NAME", "data_engineering")

N = 100000  # 100 mil registros

def generate_and_insert_data():
    print(f"Gerando {N} registros fictícios e inserindo em PostgreSQL...")
    fake = Faker()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME
    )
    cur = conn.cursor()

    # Cria a tabela se não existir
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fictitious_data (
            id SERIAL PRIMARY KEY,
            user_id INT,
            name VARCHAR(255),
            email VARCHAR(255),
            city VARCHAR(255),
            created_at TIMESTAMP
        );
    """)
    conn.commit()

    BREWERY_CITIES = [
        "Norman",
        "Austin",
        "Mount Pleasant",
        "Bend",
        "Portland",
        "San Diego",
        "Boise",
        "Denver"
    ]

    batch_size = 5000
    data_buffer = []

    for i in range(N):
        user_id = fake.random_int(min=1, max=10)
        name = fake.name()
        email = fake.email()

        # Lógica para escolher cidade:
        # 20% das vezes (ajustável), escolhemos uma cidade da lista de cervejarias
        # 80% das vezes, usamos o fake.city() normal
        if random.random() < 0.2:
            city = random.choice(BREWERY_CITIES)
        else:
            city = fake.city()

        created_at = fake.date_time_this_year()

        data_buffer.append((user_id, name, email, city, created_at))

        if (i + 1) % batch_size == 0:
            cur.executemany("""
                INSERT INTO fictitious_data (user_id, name, email, city, created_at)
                VALUES (%s, %s, %s, %s, %s);
            """, data_buffer)
            conn.commit()
            data_buffer.clear()
            print(f"{i+1} registros inseridos...")

    if data_buffer:
        cur.executemany("""
            INSERT INTO fictitious_data (user_id, name, email, city, created_at)
            VALUES (%s, %s, %s, %s, %s);
        """, data_buffer)
        conn.commit()

    cur.close()
    conn.close()
    print("Inserção de dados fictícios concluída!")

if __name__ == "__main__":
    generate_and_insert_data()
