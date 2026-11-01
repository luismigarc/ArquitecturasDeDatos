"""Crea en Redshift el usuario de sólo lectura que usa QuickSight (quicksight_ro).

Sólo puede LEER el esquema marts (mínimo privilegio). ALTER DEFAULT PRIVILEGES hace que las
tablas que dbt recrea en cada corrida sigan siendo legibles sin volver a ejecutar este script.
"""
import os
import sys

sys.path.insert(0, "/opt/airflow/src")
from ecommerce_dw.connections import connect_warehouse  # noqa: E402

USER = "quicksight_ro"


def main():
    pw = os.environ["QUICKSIGHT_RO_PASSWORD"]
    owner = os.environ["REDSHIFT_USER"]
    conn = connect_warehouse()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS marts")
        cur.execute("SELECT 1 FROM pg_user WHERE usename = %s", (USER,))
        if cur.fetchone():
            cur.execute(f"ALTER USER {USER} PASSWORD %s", (pw,))
        else:
            cur.execute(f"CREATE USER {USER} PASSWORD %s", (pw,))
        cur.execute(f"GRANT USAGE ON SCHEMA marts TO {USER}")
        cur.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA marts TO {USER}")
        cur.execute(f"ALTER DEFAULT PRIVILEGES FOR USER {owner} IN SCHEMA marts GRANT SELECT ON TABLES TO {USER}")
    conn.close()
    print(f"OK: usuario {USER} con SELECT sobre marts (contraseña en SSM /tallerdw/quicksight_password)")


if __name__ == "__main__":
    main()
