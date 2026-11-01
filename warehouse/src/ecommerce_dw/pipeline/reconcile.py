"""Conciliación de un indicador agregado: ventas totales (suma de subtotal de líneas de pedido).

    origen válido = SUM(order_items.subtotal) en RDS MySQL  -  SUM(subtotal) en cuarentena
    modelo        = SUM(marts.fct_sales.subtotal) en Redshift
Además compara el número de líneas. Si no coinciden, la tarea falla.
"""
from __future__ import annotations

from decimal import Decimal

from ..connections import connect_mysql, connect_warehouse


def source_sales_totals() -> tuple[int, Decimal]:
    conn = connect_mysql()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), coalesce(sum(subtotal), 0) FROM order_items")
            n, total = cur.fetchone()
            return int(n), Decimal(total)
    finally:
        conn.close()


def warehouse_totals(conn) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), coalesce(sum(subtotal), 0) FROM quarantine.quarantine_mysql__order_items")
        q_n, q_total = cur.fetchone()
        cur.execute("SELECT count(*), coalesce(sum(subtotal), 0) FROM marts.fct_sales")
        f_n, f_total = cur.fetchone()
        cur.execute("SELECT count(*), coalesce(sum(subtotal), 0) FROM intermediate.int_mysql__order_items__valid")
        v_n, v_total = cur.fetchone()
    return {"quarantine": (int(q_n), Decimal(q_total)), "fact": (int(f_n), Decimal(f_total)),
            "valid": (int(v_n), Decimal(v_total))}


def reconcile(source_fn=source_sales_totals, conn=None) -> dict:
    own = conn is None
    conn = conn or connect_warehouse()
    try:
        src_n, src_total = source_fn()
        w = warehouse_totals(conn)
    finally:
        if own:
            conn.close()
    expected_n = src_n - w["quarantine"][0]
    expected_total = src_total - w["quarantine"][1]
    result = {
        "indicador": "ventas_totales_subtotal",
        "origen_lineas": src_n, "origen_total": str(src_total),
        "cuarentena_lineas": w["quarantine"][0], "cuarentena_total": str(w["quarantine"][1]),
        "esperado_lineas": expected_n, "esperado_total": str(expected_total),
        "intermediate_valid_lineas": w["valid"][0], "intermediate_valid_total": str(w["valid"][1]),
        "marts_fct_sales_lineas": w["fact"][0], "marts_fct_sales_total": str(w["fact"][1]),
    }
    result["ok"] = (w["fact"][0] == expected_n and abs(w["fact"][1] - expected_total) < Decimal("0.01"))
    return result
