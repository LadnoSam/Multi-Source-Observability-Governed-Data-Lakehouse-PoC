from pyspark.sql import SparkSession

TABLE = "polaris.crypto.ethereum_prices"

spark = (
    SparkSession.builder
    .appName("check-latest-iceberg")
    .config("spark.sql.catalog.polaris.client.region", "us-east-1")
    .config("spark.sql.catalog.polaris.s3.access-key-id", "admin")
    .config("spark.sql.catalog.polaris.s3.secret-access-key", "123")
    .config("spark.sql.catalog.polaris.rest.sigv4-enabled", "false")
    .config(
        "spark.sql.catalog.polaris",
        "org.apache.iceberg.spark.SparkCatalog",
    )
    .config("spark.sql.catalog.polaris.type", "rest")
    .config(
        "spark.sql.catalog.polaris.uri",
        "http://polaris:8181/api/catalog",
    )
    .config("spark.sql.catalog.polaris.warehouse", "poc-catalog")
    .config(
        "spark.sql.catalog.polaris.credential",
        "polaris-client:oQCtaJ0XIpXzQWZl8os71q5aztSZ6YPD",
    )
    .config(
        "spark.sql.catalog.polaris.oauth2-server-uri",
        "http://keycloak:8080/realms/Viewer/protocol/openid-connect/token",
    )
    .config("spark.sql.catalog.polaris.scope", "profile email")
    .config(
        "spark.sql.catalog.polaris.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO",
    )
    .config(
        "spark.sql.catalog.polaris.s3.endpoint",
        "http://rustfs:9000",
    )
    .config("spark.sql.catalog.polaris.s3.path-style-access", "true")
    .getOrCreate()
)

try:
    row = spark.sql(f"""
        SELECT
            COUNT(*) AS rows,
            MAX(timestamp) AS latest_timestamp
        FROM {TABLE}
    """).first()

    if row["rows"] == 0:
        print("[Iceberg] ERROR table is empty")
        raise SystemExit(1)

    latest = spark.sql(f"""
        SELECT
            timestamp,
            price_usd,
            change_24h,
            market_cap,
            volume_24h
        FROM {TABLE}
        ORDER BY timestamp DESC
        LIMIT 1
    """).first()

    print(
        f"[Iceberg] OK rows={row['rows']} "
        f"latest={latest['timestamp']} "
        f"price=${latest['price_usd']} "
        f"change={latest['change_24h']}%"
    )

except Exception as e:
    print(f"[Iceberg] ERROR {e}")
    raise

finally:
    spark.stop()
