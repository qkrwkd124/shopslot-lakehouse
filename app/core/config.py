import os


def database_url() -> str:
    """Read the database URL at runtime so credentials never enter source control."""
    return os.getenv(
        "DATABASE_URL",
        "mysql+pymysql://shopslot:shopslot@mysql:3306/shopslot?charset=utf8mb4",
    )
