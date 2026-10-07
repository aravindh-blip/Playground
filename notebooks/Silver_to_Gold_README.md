# First Gold build: posted sales

Import `Silver_to_Gold_v1_Posted_Sales.ipynb` into Fabric. Create a schema-enabled
`LH_SALES_GOLD_TEST` lakehouse in the same workspace, attach it as the notebook's
default lakehouse, and attach `LH_SALES_SILVER_TEST` as an additional lakehouse.
Restart a running notebook session after changing its default lakehouse.

Keep `WRITE_GOLD = False` for the first **Run all**. This reads eight audited Silver
Delta tables, validates the proposed five Gold outputs, and reports aggregate totals.
It does not write Gold tables. Share the validation summary before the first write.

After reviewing the results and the business rules in the notebook, set
`WRITE_GOLD = True` and run all to write:

- `LH_SALES_GOLD_TEST.gold.fact_posted_sales`
- `LH_SALES_GOLD_TEST.gold.dim_customer`
- `LH_SALES_GOLD_TEST.gold.dim_item`
- `LH_SALES_GOLD_TEST.gold.dim_salesperson`
- `LH_SALES_GOLD_TEST.gold.dim_date`

`gold._gold_load_audit` stores publication events and the pinned source Delta versions.

## Rules and validation

- Grain: one company/document type/document number/line number. Full replacement.
- Posting date and document salesperson come from the header. Inconsistent line/header
  dates or customer attribution fail before publication, except type 0 comment lines
  whose line date is exactly `1753-01-01`. These use the valid header posting date,
  retain `source_line_posting_date`, and set `line_posting_date_defaulted = true`.
  Null dates and other date mismatches still fail, including on comment lines.
- Credit amounts and quantities are negated, including negative correction lines.
  Original values are retained. No absolute-value conversion is applied.
- Pre-VAT and VAT-inclusive amounts remain separate. Amounts stay in document
  currency; a blank currency code means the company's unspecified local currency.
  No currency conversion is performed. Do not aggregate across currencies or quantity units.
- All line types remain available. `is_financial_line` identifies non-comment lines;
  finance must approve revenue scope and cancellation/prepayment treatment.
- Missing master records become flagged dimension placeholders. No inner dimension
  join silently removes sales. Dimensions are current-state (Type 1).
- Invoice and credit document counts and exact decimal totals reconcile against Silver.
  Keys and fact/dimension relationships are checked, then persisted outputs are
  compared against the validated in-memory outputs when write mode is enabled.
- Existing target schema mismatches fail before output replacements.

The Silver source checks currently require the successful full-load audits produced
by the v6 notebook. Source versions are pinned individually, not as a cross-table
source-system transaction. Prevent overlapping writers. The original 12 Silver tables
and later five-table addition can have distinct batch IDs; lineage is verified per table.

Gold writes are separate Delta transactions, not an atomic multi-table publication.
A failed write can leave partial output updates. Schedule semantic model refresh only
on notebook success. Direct Lake requires additional publication coordination before
production use. No production model, schedule or report is modified here.

The first build excludes SharePoint, historical actuals, budget, forecast, ACL data,
open orders, item/customer extensions, and item-reference enrichment. This notebook
provides the first posted-sales foundation, not complete parity with the existing report.

## Verification

Fourteen synthetic tests passed using local PySpark 3.5.3. They cover end-to-end transformation
and reconciliation, overlapping invoice/credit numbers, negative credits, decimal
precision, placeholder members, leap dates, and rejection of duplicate keys, orphan
lines, inconsistent dates/customers, null amounts, missing columns and altered totals. Regression tests also cover the observed comment-line
default date and rejection of financial sentinel dates, null dates and other mismatches.
Notebook structure and every code cell were also validated locally.

Fabric lakehouse permissions, Delta source access, destination writes and audit
persistence remain to be validated in Fabric using the actual data.

To run the local tests (Java required):

```sh
python -m pip install pyspark==3.5.3 pytest==8.3.5
SPARK_LOCAL_IP=127.0.0.1 python -m pytest -q tests/test_gold_notebook.py
```
