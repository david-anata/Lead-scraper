from sales_support_agent.models.database import _normalize_db_url


def test_postgres_schemes_name_the_installed_driver():
    assert _normalize_db_url("postgres://u:p@h/db") == "postgresql+psycopg2://u:p@h/db"
    assert _normalize_db_url("postgresql://u:p@h/db") == "postgresql+psycopg2://u:p@h/db"


def test_explicit_driver_and_sqlite_untouched():
    assert _normalize_db_url("postgresql+psycopg://u@h/db") == "postgresql+psycopg://u@h/db"
    assert _normalize_db_url("sqlite:///x.db") == "sqlite:///x.db"
