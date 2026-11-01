"""Ejecuta SQL en Redshift (dentro de la EC2) e imprime el resultado como tabla.

    python /opt/airflow/tools/sql.py "select count(*) from marts.fct_sales"
    python /opt/airflow/tools/sql.py -f /opt/airflow/sql/validacion_kpi.sql
"""
import argparse
import sys

sys.path.insert(0, "/opt/airflow/src")
from ecommerce_dw.connections import connect_warehouse  # noqa: E402


def split_sql(text: str) -> list[str]:
    lines = [ln for ln in text.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def show(cur):
    if cur.description is None:
        print(f"({cur.rowcount} filas afectadas)")
        return
    headers = [d[0] for d in cur.description]
    rows = [["" if v is None else str(v) for v in r] for r in cur.fetchall()]
    widths = [max([len(h)] + [len(r[i]) for r in rows]) for i, h in enumerate(headers)]
    print(" | ".join(h.ljust(w) for h, w in zip(headers, widths)))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(" | ".join(v.ljust(w) for v, w in zip(r, widths)))
    print(f"({len(rows)} filas)\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("sql", nargs="?")
    ap.add_argument("-f", "--file")
    a = ap.parse_args()
    text = open(a.file, encoding="utf-8").read() if a.file else a.sql
    conn = connect_warehouse()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for stmt in split_sql(text):
                print(">>", " ".join(stmt.split())[:160])
                cur.execute(stmt)
                show(cur)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
