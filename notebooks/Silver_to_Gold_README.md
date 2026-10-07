# Silver to Gold: one maintained notebook

Continue using your existing Fabric Gold notebook. The canonical repository file is
`Silver_to_Gold_v1_Posted_Sales.ipynb`; its filename is retained while its functionality
now includes posted sales, open orders and shipped-not-invoiced data. The separate v2
copy has been retired from the current branch (previous commits remain in Git history).

## Update the existing Fabric notebook

1. In your existing notebook, replace the entire **Configuration** code cell with
   [`cells/gold_configuration.py`](cells/gold_configuration.py).
2. Replace the entire **Transformations, validation, source versioning and publication**
   code cell (the one beginning `def configure_spark():`) with
   [`cells/gold_transformations.py`](cells/gold_transformations.py).
3. Keep the final `run_gold()` cell. Keep all existing lakehouse attachments.
4. Keep `WRITE_GOLD=False` and **Run all** after Silver finishes. Do not run only the
   final cell, which could reuse old function definitions from the existing session.
5. Review the seven validated counts and per-currency/UOM totals before enabling writes.
6. After validation, set `WRITE_GOLD=True` and Run all to refresh and verify all seven
   Gold tables. New order facts are then ready to import into the existing test model.

The uploaded export included the comment-date correction but predated the customer-code
correction applied manually in Fabric. This update retains the date fix and restores the
confirmed blank-customer comment-line handling, including its original-value columns
and default flags. It preserves existing posted-sales/dimension output schemas.

The new sources are `sales_header`, `sales_line` and `sales_line_ext`. The header
extension has no fields needed for the current calculations. All 11 inputs must have
successful full-load audits from the Silver notebook. The source tables are read at
pinned Delta versions; concurrent upstream writes are rejected before Gold publication.

## Open orders: the confirmed rule

- Only Sales Order document type `1` is eligible.
- `remaining_quantity = quantity - quantity_shipped` (the existing model's definition).
- `fact_open_sales_orders` contains **only remaining_quantity > 1**.
- `open_order_value = remaining_quantity * unit_price`.
- Keep fractions. The old M query converted remaining quantity to an integer; the notebook
  deliberately applies the user's threshold to the actual decimal balance.
- The product is stored as decimal(38,20), rounded half up only beyond 20 fractional
  places. Quantities are not rounded to integers. Overflow fails rather than truncating.
- No line-discount, invoice-discount, VAT or currency adjustment is added to the formula.
  `prices_including_vat`, `currency_code` and `unit_of_measure_code` remain visible.
  Do not label this formula as universally excluding VAT or sum across currencies.
- `source_outstanding_quantity` is retained for comparison; it does not replace the formula.
- All order statuses and line types with qualifying quantities remain; no additional
  business exclusion is assumed.
- Extension joins include document type, document number and line number. Customer PO
  and production order number are optional enrichments and cannot multiply fact rows.

## Shipped but not invoiced

`fact_shipped_not_invoiced` contains order lines where `qty_shipped_not_invoiced > 0`.
This is independent of the open-order >1 threshold. Fully shipped orders awaiting an
invoice must remain visible here. A partially shipped order line can appear in both
facts: each fact describes a different quantity.

The fact retains source shipped quantity/amount, LCY amount, and LCY amount excluding
VAT. These are not recalculated using the open-order formula. Distinct order counts
across facts are not additive. No combined sales-projection value is introduced until
its currency, discount and VAT bases are agreed.

## Date and snapshot behavior

Both new facts use the sales line's `planned_shipment_date` for planning. This is not
an actual shipment-date history. The date dimension expands to cover valid future dates.
A missing/default/out-of-range planning date retains its original value in
`source_planned_shipment_date`, is flagged, and gets a null date key. The record remains
in all-time totals; Power BI shows it under its blank date member and ordinary date
slicers exclude it. Fix dates in the source rather than assigning an arbitrary date.

Gold is a current snapshot, fully replaced on each run. Empty order results are valid
and clear previously loaded orders. `_gold_processed_at` provides processing time.
Historical backlog trends need a separate dated-snapshot design.

## Power BI model setup after the Fabric write succeeds

Import `gold.fact_open_sales_orders` and `gold.fact_shipped_not_invoiced` into the
existing test model. For each new fact, add these active, single-direction relationships:

| Dimension (1 side) | New fact (* side) |
| --- | --- |
| dim_customer.customer_key | bill_to_customer_key |
| dim_item.item_key | item_key |
| dim_salesperson.salesperson_key | salesperson_key |
| dim_date.date_key | planned_shipment_date_key |

Keep existing posted-sales relationships. Do not connect fact tables directly.
Refresh all shared dimensions together with the three facts after a successful Gold run.
No schedules, report definitions or semantic models are changed by importing the notebook.

## Base measures

The physical fact already enforces >1. These measures also state that rule explicitly,
so every derived current-month/year/customer/item measure can reuse them consistently.
The names below assume Power BI imports the table as `gold fact_open_sales_orders`.

```dax
Open Orders Value =
COALESCE(
    CALCULATE(
        SUM('gold fact_open_sales_orders'[open_order_value]),
        KEEPFILTERS('gold fact_open_sales_orders'[remaining_quantity] > 1)
    ),
    0
)
```

```dax
Open Orders Remaining Quantity =
COALESCE(
    CALCULATE(
        SUM('gold fact_open_sales_orders'[remaining_quantity]),
        KEEPFILTERS('gold fact_open_sales_orders'[remaining_quantity] > 1)
    ),
    0
)
```

```dax
Open Orders Count =
COALESCE(
    CALCULATE(
        DISTINCTCOUNT('gold fact_open_sales_orders'[order_key]),
        KEEPFILTERS('gold fact_open_sales_orders'[remaining_quantity] > 1)
    ),
    0
)
```

```dax
Open Order Lines =
COALESCE(
    CALCULATE(
        COUNTROWS('gold fact_open_sales_orders'),
        KEEPFILTERS('gold fact_open_sales_orders'[remaining_quantity] > 1)
    ),
    0
)
```

Group quantity visuals by unit of measure and amount visuals by currency. The raw
`ordered_quantity` is retained as an attribute but is not the remaining-quantity measure.
Use date slicers for monthly/yearly analysis; any explicit time-intelligence measures
should reference these base measures so the >1 rule remains in effect.

For separate shipped-not-invoiced analysis:

```dax
Shipped Not Invoiced Quantity =
COALESCE(
    SUM('gold fact_shipped_not_invoiced'[shipped_not_invoiced_quantity]),
    0
)
```

```dax
Shipped Not Invoiced (Excl VAT, LCY) =
COALESCE(
    SUM('gold fact_shipped_not_invoiced'[shipped_not_invoiced_excl_vat_lcy]),
    0
)
```

## Validation

All 33 synthetic Spark tests passed against the merged notebook on PySpark 3.5.3.
They cover posted-sales regressions and exercise >1 boundaries including fractional quantities, quotes
and returns, header/extension join keys, partial and complete shipments, exact decimal
arithmetic, missing master placeholders, future/unknown planning dates, invalid inputs,
and empty backlogs. The combined build also checks posted-sales reconciliation.
Actual Fabric execution, SQL endpoint visibility and model refresh require user validation.

```sh
python -m pip install pyspark==3.5.3 pytest==8.3.5
SPARK_LOCAL_IP=127.0.0.1 python -m pytest -q tests/test_gold_notebook.py tests/test_gold_orders.py
```
