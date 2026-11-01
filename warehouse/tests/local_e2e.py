"""Prueba end-to-end SIN AWS: fuentes simuladas en memoria (mismo generador de datos),
S3 simulado (moto) y PostgreSQL como sustituto de Redshift para correr el proyecto dbt real.

Escenarios (3.5): carga inicial, registros defectuosos, repetición de un lote exitoso, nuevo lote
incremental, cambio de atributo historizado (SCD2), fallo controlado y reejecución.

Uso (requiere un PostgreSQL local en localhost:5432 con base 'dev'):
    python tests/local_e2e.py
"""
from __future__ import annotations

import argparse
import copy
import os
import subprocess
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
os.environ.update(
    AWS_ACCESS_KEY_ID="test", AWS_SECRET_ACCESS_KEY="test", AWS_DEFAULT_REGION="us-east-1",
    AWS_REGION="us-east-1", DW_BUCKET="test-dw", WAREHOUSE_ENGINE="postgres", DBT_TARGET="pgtest",
    REDSHIFT_HOST=os.getenv("PGHOST", "localhost"), REDSHIFT_PORT=os.getenv("PGPORT", "5432"),
    REDSHIFT_DB="dev", REDSHIFT_USER="postgres", REDSHIFT_PASSWORD="", REDSHIFT_SSLMODE="disable",
)

import boto3  # noqa: E402
import psycopg2  # noqa: E402
from moto import mock_aws  # noqa: E402

from ecommerce_dw.pipeline import extract, tasks  # noqa: E402
from ecommerce_dw.pipeline.entities import ENTITIES  # noqa: E402
from ecommerce_dw.sources import append_batch as ab  # noqa: E402
from ecommerce_dw.sources import generate_data as gd  # noqa: E402

DBT = [str(Path(sys.executable).parent / "dbt")]
DBT_DIR = ROOT / "src" / "dbt"


class FakeSources:
    """Imita RDS MySQL y DocumentDB con el mismo generador sembrado del proyecto local."""

    def __init__(self, bad_records=2):
        args = argparse.Namespace(customer_count=50, product_count=25, order_count=100, review_count=150,
                                  event_count=500, seed=42, bad_records=bad_records)
        rng, fake, customers, products, orders, details, start = gd.build_rows(args)
        bad = gd.build_bad_records(args, start)
        reviews, events = gd.build_mongo_documents(args, rng, fake, customers, products, start)
        self.tables = {
            "customers": customers + bad["mysql_customers"],
            "products": products + bad["mysql_products"],
            "orders": orders + bad["mysql_orders"],
            "order_items": details + bad["mysql_order_items"],
            "reviews": reviews + bad["mongo_reviews"],
            "navigation_events": events + bad["mongo_events"],
        }

    def rows(self, entity, watermark):
        data = sorted(self.tables[entity.name], key=lambda r: r[entity.key])
        for r in data:
            if watermark is not None and r[entity.key] <= watermark:
                continue
            if entity.source == "mysql":
                yield {c.name: copy.deepcopy(r.get(c.name)) for c in entity.columns}
            else:
                yield extract.flatten_mongo(entity.name, copy.deepcopy(r))

    def sales_totals(self):
        items = self.tables["order_items"]
        return len(items), sum((Decimal(str(i["subtotal"])) for i in items), Decimal("0"))

    def append_batch(self, batch_id):
        from datetime import timedelta
        import random
        from faker import Faker
        day = datetime(2026, 1, 1) + timedelta(days=batch_id - 1)
        customers = [{"customer_id": c["customer_id"], "name": c["name"]} for c in self.tables["customers"][:3]]
        products = [{k: p[k] for k in ("product_id", "name", "category", "price")} for p in self.tables["products"][:3]]
        new_orders = 0
        for order_id, order, item_id, item in ab.build_order_rows(batch_id, day, customers, products):
            if any(o["order_id"] == order_id for o in self.tables["orders"]):
                continue
            cid, odate, status, channel, pay, total = order
            self.tables["orders"].append(dict(order_id=order_id, customer_id=cid, order_date=odate, status=status,
                                              sales_channel=channel, payment_method=pay, total=total))
            oid, pid, qty, price, sub = item
            self.tables["order_items"].append(dict(order_item_id=item_id, order_id=oid, product_id=pid,
                                                   quantity=qty, unit_price=price, subtotal=sub))
            new_orders += 1
        fake = Faker("es_CO")
        fake.seed_instance(batch_id)
        reviews, events = ab.build_mongo_documents(batch_id, day, customers, products, fake, random.Random(batch_id))
        for coll, docs in (("reviews", reviews), ("navigation_events", events)):
            for d in docs:
                self.tables[coll] = [x for x in self.tables[coll] if x["_id"] != d["_id"]] + [d]
        return new_orders

    def simulate_change(self, customer_id=1, city="Medellín", department="Antioquia", product_id=1,
                        category="Hogar", changed_at=datetime(2025, 7, 1)):
        for c in self.tables["customers"]:
            if c["customer_id"] == customer_id:
                c.update(city=city, department=department, updated_at=changed_at)
        for p in self.tables["products"]:
            if p["product_id"] == product_id:
                p.update(category=category, updated_at=changed_at)


def dbt(*args):
    r = subprocess.run(DBT + list(args) + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)],
                       capture_output=True, text=True, env=os.environ)
    if r.returncode != 0:
        print(r.stdout[-6000:], r.stderr[-3000:])
        raise RuntimeError(f"dbt {' '.join(args)} falló")
    return r.stdout


def pg():
    return psycopg2.connect(host=os.environ["REDSHIFT_HOST"], port=int(os.environ["REDSHIFT_PORT"]),
                            dbname="dev", user="postgres")


def q(sql):
    with pg() as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def run_pipeline(src: FakeSources, run_id, reprocess=None, mode="incremental", fail_at=None):
    s3 = tasks.get_s3()
    ctx = tasks.build_ctx(run_id, reprocess, mode)
    failed = []
    try:
        for e in ENTITIES:
            tasks.extract_entity(ctx, e.name, s3=s3, row_source=src.rows)
        tasks.register_batch(ctx, s3=s3)
        tasks.load_raw(ctx, s3=s3, conn=pg())
        for step, cmd, sel in (("dbt_seed", "seed", None), ("dbt_run_intermediate", "run", "path:models/intermediate"),
                               ("dbt_run_quarantine", "run", "path:models/quarantine"), ("dbt_snapshot", "snapshot", None),
                               ("dbt_run_marts", "run", "path:models/marts"), ("dbt_test", "test", None)):
            if fail_at == step:
                raise RuntimeError(f"fallo controlado en {step}")
            dbt(cmd, *(["--select", sel] if sel else []))
        tasks.reconcile_kpi(ctx, s3=s3, source_fn=src.sales_totals, conn=pg())
    except Exception as e:  # noqa: BLE001
        print("   !! falló:", e)
        failed.append(str(e))
    st = tasks.record_batch(ctx, failed, s3=s3)
    return ctx, st


def snapshot_state():
    return {
        "fct_sales": q("select count(*), count(distinct sale_sk), sum(subtotal) from marts.fct_sales")[0],
        "fct_reviews": q("select count(*), count(distinct review_sk) from marts.fct_reviews")[0],
        "fct_nav": q("select count(*), count(distinct navigation_event_sk) from marts.fct_navigation_events")[0],
        "dim_customer": q("select count(*) from marts.dim_customer")[0],
        "dim_product": q("select count(*) from marts.dim_product")[0],
        "content": q("select md5(string_agg(sale_sk || customer_sk || product_sk || subtotal::text, ',' order by sale_sk)) from marts.fct_sales")[0],
    }


def main():
    with mock_aws():
        boto3.client("s3").create_bucket(Bucket="test-dw")
        with pg() as c, c.cursor() as cur:
            cur.execute("drop schema if exists raw, intermediate, quarantine, marts, snapshots, seeds cascade")
        if not (DBT_DIR / "dbt_packages" / "dbt_utils").exists():
            print(dbt("deps")[-200:])
        src = FakeSources(bad_records=2)

        print("\n== 1+2. Carga inicial (con registros defectuosos)")
        ctx1, st = run_pipeline(src, "manual__2026-11-01T10:00:00+00:00")
        assert st["status"] == "PROCESSED", st
        s1 = snapshot_state()
        print("   ", s1)
        rec = tasks.get_s3().get_json(f"control/batches/batch_id={ctx1['batch_id']}/reconciliation.json")
        print("    conciliación:", {k: rec[k] for k in ("origen_total", "cuarentena_total", "marts_fct_sales_total", "ok")})
        quarantine = q("select 'customers', rejection_reason, _batch_id from quarantine.quarantine_mysql__customers "
                       "union all select 'items', rejection_reason, _batch_id from quarantine.quarantine_mysql__order_items "
                       "union all select 'reviews', rejection_reason, _batch_id from quarantine.quarantine_mongodb__reviews")
        print("    cuarentena (muestra):", quarantine[:4], "total", len(quarantine))
        assert all(r[2] == ctx1["batch_id"] for r in quarantine)
        assert s1["fct_sales"][0] == s1["fct_sales"][1]

        print("\n== 3. Repetición del lote exitoso (reprocesar desde S3)")
        ctx, st = run_pipeline(src, "manual__2026-11-01T10:30:00+00:00", reprocess=ctx1["batch_id"])
        assert st["status"] == "PROCESSED"
        assert snapshot_state() == s1, (snapshot_state(), s1)
        print("    sin duplicados ni versiones nuevas ✓")

        print("\n== 3b. Lote nuevo sin cambios en la fuente (extracción incremental vacía)")
        ctx, st = run_pipeline(src, "manual__2026-11-01T11:00:00+00:00")
        m = tasks.get_s3().get_json(f"control/batches/batch_id={ctx['batch_id']}/manifest.json")
        print("    filas extraídas:", {k: v["rows"] for k, v in m["entities"].items()})
        assert m["entities"]["orders"]["rows"] == 0 and m["entities"]["customers"]["rows"] > 0
        assert snapshot_state() == s1

        print("\n== 4. Nuevo lote incremental (append_batch 1)")
        assert src.append_batch(1) == 3
        ctx4, st = run_pipeline(src, "manual__2026-11-01T12:00:00+00:00")
        assert st["status"] == "PROCESSED", st
        s4 = snapshot_state()
        print("   ", s4)
        assert s4["fct_sales"][0] == s1["fct_sales"][0] + 3
        assert s4["fct_reviews"][0] == s1["fct_reviews"][0] + 3
        assert s4["fct_nav"][0] == s1["fct_nav"][0] + 3
        m = tasks.get_s3().get_json(f"control/batches/batch_id={ctx4['batch_id']}/manifest.json")
        print("    incremental:", {k: (v["mode"], v["watermark_from"], v["rows"]) for k, v in m["entities"].items()})

        print("\n== 4b. Repetir el mismo append (idempotente en la fuente) + nueva corrida")
        assert src.append_batch(1) == 0
        run_pipeline(src, "manual__2026-11-01T12:30:00+00:00")
        assert snapshot_state() == s4

        print("\n== 5. Cambio en atributo historizado (SCD2)")
        src.simulate_change()
        ctx5, st = run_pipeline(src, "manual__2026-11-01T13:00:00+00:00")
        assert st["status"] == "PROCESSED", st
        hist = q("select customer_id, city, valid_from, valid_to, is_current from marts.dim_customer "
                 "where customer_id = 1 order by valid_from")
        print("    cliente 1:", hist)
        assert len(hist) == 2 and hist[0][4] is False and hist[1][4] is True and hist[0][3] == hist[1][2]
        prod = q("select product_id, category, is_current from marts.dim_product where product_id = 1 order by valid_from")
        print("    producto 1:", prod)
        s5 = snapshot_state()
        run_pipeline(src, "manual__2026-11-01T13:30:00+00:00")
        assert snapshot_state() == s5, "repetir no debe crear versiones nuevas"
        print("    repetir el cambio no crea versiones nuevas ✓")

        print("\n== 6. Fallo controlado en dbt_run_marts y reejecución")
        assert src.append_batch(2) == 3
        ctx6, st = run_pipeline(src, "manual__2026-11-01T14:00:00+00:00", fail_at="dbt_run_marts")
        assert st["status"] == "FAILED"
        wm_before = tasks.get_s3().get_json("control/state/watermarks.json")["watermarks"]
        ctx6b, st = run_pipeline(src, "manual__2026-11-01T14:00:00+00:00")   # Clear del mismo run
        assert st["status"] == "PROCESSED"
        s6 = snapshot_state()
        assert s6["fct_sales"][0] == s5["fct_sales"][0] + 3 and s6["fct_sales"][0] == s6["fct_sales"][1]
        print("    marca de agua antes del retry:", wm_before["orders"], "después:",
              tasks.get_s3().get_json("control/state/watermarks.json")["watermarks"]["orders"])
        print("    recuperación sin pérdida ni duplicados ✓", s6["fct_sales"])

        print("\n== 7. Extracción full (recuperación) no duplica")
        run_pipeline(src, "manual__2026-11-01T15:00:00+00:00", mode="full")
        assert snapshot_state() == s6
        print("    ✓")
        print("\nTODO OK")


if __name__ == "__main__":
    main()
