from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://postgres:210594@localhost:5432/beverage_sales",
    echo=True,
)

with engine.connect() as conn:
    result = conn.execute(text("select 'Hello'"))

    print(result.all())
