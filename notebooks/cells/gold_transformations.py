def configure_spark():
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    # Consistent with the validated Silver notebook's calendar convention.
    for suffix in ["datetimeRebaseModeInRead", "int96RebaseModeInRead",
                   "datetimeRebaseModeInWrite", "int96RebaseModeInWrite"]:
        spark.conf.set("spark.sql.parquet." + suffix, "CORRECTED")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")


def require_columns(df, names, label):
    missing = sorted(set(names) - set(df.columns))
    if missing:
        raise ValueError(f"{label}: missing columns {missing}")


def reject_rows(df, predicate, label):
    if df.filter(predicate).limit(1).count():
        raise ValueError(label)


def validate_key(df, keys, label):
    require_columns(df, keys, label)
    invalid = F.lit(False)
    for k in keys:
        invalid = invalid | F.col(k).isNull() | (F.trim(F.col(k).cast("string")) == "")
    reject_rows(df, invalid, f"{label}: blank/null key")
    if df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{label}: duplicate key {keys}")


def text_code(column):
    # Strip surrounding ordinary spaces; do not change case or leading zeroes.
    return F.trim(column.cast("string"))


def relation_key(column):
    # Prefix real keys so source values cannot collide with the unknown member.
    c = text_code(column)
    return F.when(c.isNull() | (c == ""), F.lit("U:")).otherwise(F.concat(F.lit("K:"), c))


def require_money(df, names, label):
    require_columns(df, names, label)
    for name in names:
        if not isinstance(df.schema[name].dataType, DecimalType):
            raise ValueError(f"{label}.{name}: expected an exact decimal type")
        reject_rows(df, F.col(name).isNull(), f"{label}.{name}: null numeric value")


def build_document(lines, headers, document_type):
    if document_type not in {"INVOICE", "CREDIT_MEMO"}:
        raise ValueError("Unsupported document type")
    require_columns(lines, ["document_no", "line_no", "type", "no", "description",
                           "quantity", "quantity_base", "unit_of_measure_code", "amount",
                           "amount_including_vat", "posting_date", "bill_to_customer_no",
                           "sell_to_customer_no", "_silver_run_id"], document_type + " lines")
    require_columns(headers, ["no", "posting_date", "document_date", "bill_to_customer_no",
                             "sell_to_customer_no", "salesperson_code", "currency_code",
                             "currency_factor", "correction", "marked_as_canceled",
                             "prepayment_invoice" if document_type == "INVOICE" else "prepayment_credit_memo",
                             "_silver_run_id"], document_type + " headers")
    require_money(lines, ["quantity", "quantity_base", "amount", "amount_including_vat"], document_type)
    l = lines.withColumn("document_no", text_code(F.col("document_no")))
    h = headers.withColumn("no", text_code(F.col("no")))
    validate_key(l, ["document_no", "line_no"], document_type + " lines")
    validate_key(h, ["no"], document_type + " headers")
    reject_rows(l, F.col("type").isNull(), document_type + ": missing line type")
    if l.join(h.select(F.col("no").alias("document_no")), "document_no", "left_anti").limit(1).count():
        raise ValueError(document_type + ": line without matching header")
    j = l.alias("l").join(h.alias("h"), F.col("l.document_no") == F.col("h.no"), "inner")
    reject_rows(j, F.to_date(F.col("h.posting_date")).isNull(), document_type + ": missing posting date")
    reject_rows(j, ~F.year(F.col("h.posting_date")).between(MIN_POSTING_YEAR, MAX_POSTING_YEAR),
                document_type + ": posting year outside configured range")
    # Observed BC comment lines may carry the SQL default date 1753-01-01.
    # Accept only that specific sentinel on type 0; financial lines remain strict.
    line_date = F.to_date(F.col("l.posting_date"))
    comment_default_date = F.coalesce(
        (F.col("l.type") == 0) & (line_date == F.lit(date(1753, 1, 1))),
        F.lit(False),
    )
    date_mismatch = line_date.isNull() | (line_date != F.to_date(F.col("h.posting_date")))
    reject_rows(j, date_mismatch & ~comment_default_date,
                document_type + ": line/header posting dates differ")
    # Comment lines can omit customer codes regardless of their posting date.
    # Only blank/null comment-line codes may inherit a populated header code.
    customer_defaulted = {}
    for c in ["bill_to_customer_no", "sell_to_customer_no"]:
        line_key = relation_key(F.col("l." + c))
        header_key = relation_key(F.col("h." + c))
        customer_defaulted[c] = (
            (F.col("l.type") == 0) & (line_key == "U:") & (header_key != "U:")
        )
        reject_rows(j, (~line_key.eqNullSafe(header_key)) & ~customer_defaulted[c],
                    document_type + f": line/header {c} differs")
    credit = document_type == "CREDIT_MEMO"
    def signed(name):
        # Unary negation preserves decimal precision and correctly reverses negative credits.
        return -F.col("l." + name) if credit else F.col("l." + name)
    prepayment = "prepayment_credit_memo" if credit else "prepayment_invoice"
    return j.select(
        F.lit(COMPANY_KEY).alias("company_key"), F.lit(document_type).alias("document_type"),
        F.col("l.document_no"), F.col("l.line_no"), F.col("l.type").alias("line_type"),
        (F.col("l.type") != 0).alias("is_financial_line"),
        F.col("l.no").alias("source_line_no"), F.col("l.description"),
        line_date.alias("source_line_posting_date"),
        comment_default_date.alias("line_posting_date_defaulted"),
        F.to_date(F.col("h.posting_date")).alias("posting_date"),
        F.date_format(F.col("h.posting_date"), "yyyyMMdd").cast("int").alias("posting_date_key"),
        F.to_date(F.col("h.document_date")).alias("document_date"),
        F.col("l.bill_to_customer_no").alias("source_line_bill_to_customer_no"),
        F.col("l.sell_to_customer_no").alias("source_line_sell_to_customer_no"),
        customer_defaulted["bill_to_customer_no"].alias("bill_to_customer_defaulted"),
        customer_defaulted["sell_to_customer_no"].alias("sell_to_customer_defaulted"),
        relation_key(F.col("h.bill_to_customer_no")).alias("bill_to_customer_key"),
        relation_key(F.col("h.sell_to_customer_no")).alias("sell_to_customer_key"),
        relation_key(F.col("h.salesperson_code")).alias("salesperson_key"),
        F.when(F.col("l.type") == ITEM_LINE_TYPE, relation_key(F.col("l.no")))
            .otherwise(F.lit("N:")).alias("item_key"),
        F.coalesce(text_code(F.col("h.currency_code")), F.lit("")).alias("currency_code"),
        F.col("h.currency_factor"), F.col("h.correction").alias("source_correction"),
        F.col("h.marked_as_canceled").alias("source_marked_as_canceled"),
        F.col("h." + prepayment).alias("source_prepayment"),
        F.col("l.unit_of_measure_code"),
        F.col("l.quantity").alias("source_quantity"), signed("quantity").alias("signed_quantity"),
        F.col("l.quantity_base").alias("source_quantity_base"), signed("quantity_base").alias("signed_quantity_base"),
        F.col("l.amount").alias("source_amount_excl_vat"), signed("amount").alias("signed_amount_excl_vat"),
        F.col("l.amount_including_vat").alias("source_amount_incl_vat"),
        signed("amount_including_vat").alias("signed_amount_incl_vat"),
        F.col("l._silver_run_id").alias("_silver_line_run_id"),
        F.col("h._silver_run_id").alias("_silver_header_run_id"),
    )


def add_missing_members(dim, key, name_column, references):
    # Keep every fact key, including deleted master records, with an explicit placeholder.
    wanted = references.select(F.col(key)).distinct()
    missing = wanted.join(dim.select(key), key, "left_anti")
    placeholders = missing.select(*[
        F.col(key) if c == key else
        F.lit(COMPANY_KEY).alias(c) if c == "company_key" else
        F.when(F.col(key) == "N:", F.lit("Not applicable"))
         .when(F.col(key) == "U:", F.lit("Unknown / unassigned"))
         .otherwise(F.concat(F.lit("Missing master: "), F.col(key))).alias(c) if c == name_column else
        F.lit(True).alias(c) if c == "is_placeholder" else
        F.lit(None).cast(dim.schema[c].dataType).alias(c)
        for c in dim.columns
    ])
    return dim.unionByName(placeholders)


def build_dimensions(sources, fact):
    customer = sources["customer"]
    require_columns(customer, ["no", "name", "city", "county", "post_code", "country_region_code",
                               "salesperson_code", "customer_posting_group", "blocked"], "customer")
    customer = customer.withColumn("no", text_code(F.col("no")))
    validate_key(customer, ["no"], "customer")
    customers = customer.select(
        F.lit(COMPANY_KEY).alias("company_key"), relation_key(F.col("no")).alias("customer_key"),
        F.col("no").alias("customer_no"), F.col("name").alias("customer_name"),
        "city", "county", "post_code", "country_region_code", "customer_posting_group", "blocked",
        F.col("salesperson_code").alias("current_salesperson_code"), F.lit(False).alias("is_placeholder"))
    refs = fact.select(F.col("bill_to_customer_key").alias("customer_key")).unionByName(
        fact.select(F.col("sell_to_customer_key").alias("customer_key")))
    customers = add_missing_members(customers, "customer_key", "customer_name", refs)

    item, category = sources["item"], sources["item_category"]
    require_columns(item, ["no", "description", "description_2", "base_unit_of_measure",
                          "item_category_code", "net_weight", "gross_weight", "blocked"], "item")
    require_columns(category, ["code", "description", "parent_category"], "item_category")
    item = item.withColumn("no", text_code(F.col("no")))
    category = category.withColumn("code", text_code(F.col("code")))
    validate_key(item, ["no"], "item")
    validate_key(category, ["code"], "item_category")
    items = item.alias("i").join(category.alias("c"), text_code(F.col("i.item_category_code")) == F.col("c.code"), "left").select(
        F.lit(COMPANY_KEY).alias("company_key"), relation_key(F.col("i.no")).alias("item_key"),
        F.col("i.no").alias("item_no"), F.col("i.description").alias("item_description"),
        F.col("i.description_2").alias("item_description_2"), F.col("i.base_unit_of_measure"),
        F.col("i.item_category_code"), F.col("c.description").alias("item_category_description"),
        F.col("c.parent_category"), F.col("i.net_weight"), F.col("i.gross_weight"), F.col("i.blocked"),
        (text_code(F.col("i.item_category_code")).isNotNull() & (text_code(F.col("i.item_category_code")) != "")
         & F.col("c.code").isNull()).alias("category_missing"), F.lit(False).alias("is_placeholder"))
    items = add_missing_members(items, "item_key", "item_description", fact.select("item_key"))

    rep = sources["salesperson_purchaser"]
    require_columns(rep, ["code", "name", "blocked"], "salesperson_purchaser")
    rep = rep.withColumn("code", text_code(F.col("code")))
    validate_key(rep, ["code"], "salesperson_purchaser")
    reps = rep.select(F.lit(COMPANY_KEY).alias("company_key"), relation_key(F.col("code")).alias("salesperson_key"),
                      F.col("code").alias("salesperson_code"), F.col("name").alias("salesperson_name"),
                      "blocked", F.lit(False).alias("is_placeholder"))
    reps = add_missing_members(reps, "salesperson_key", "salesperson_name", fact.select("salesperson_key"))
    return {"dim_customer": customers, "dim_item": items, "dim_salesperson": reps}


def build_dates(fact, today=None):
    today = today or datetime.now(timezone.utc).date()
    bounds = fact.agg(F.min("posting_date").alias("lo"), F.max("posting_date").alias("hi")).first()
    if bounds["lo"] is None:
        raise ValueError("No posted sales rows; empty fact publication is blocked")
    first_year = min(bounds["lo"].year, today.year)
    last_year = max(bounds["hi"].year, today.year)
    df = spark.range(1).select(F.explode(F.sequence(F.lit(date(first_year, 1, 1)),
                                                   F.lit(date(last_year, 12, 31)))).alias("date"))
    return df.select("date", F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
                     F.year("date").alias("year"), F.quarter("date").alias("quarter"),
                     F.month("date").alias("month_number"), F.date_format("date", "MMMM").alias("month_name"),
                     F.date_format("date", "yyyy-MM").alias("year_month"), F.last_day("date").alias("month_end"),
                     F.dayofmonth("date").alias("day_of_month"))


def check_relationships(outputs):
    fact = outputs["fact_posted_sales"]
    links = [("bill_to_customer_key", "dim_customer", "customer_key"),
             ("sell_to_customer_key", "dim_customer", "customer_key"),
             ("item_key", "dim_item", "item_key"),
             ("salesperson_key", "dim_salesperson", "salesperson_key"),
             ("posting_date_key", "dim_date", "date_key")]
    for fk, table, pk in links:
        target = outputs[table].select(F.col(pk).alias(fk))
        if fact.select(fk).distinct().join(target, fk, "left_anti").limit(1).count():
            raise ValueError(f"Unresolved relationship: {fk} -> {table}.{pk}")


def reconcile_document(lines, headers, actual, document_type):
    # Independent source rollup at document grain; currencies never combined.
    original = lines.alias("l").join(headers.alias("h"), F.col("l.document_no") == F.col("h.no"), "inner")
    groups = ["document_no", "currency_code"]
    raw = original.select(F.col("l.document_no"),
        F.coalesce(text_code(F.col("h.currency_code")), F.lit("")).alias("currency_code"),
        F.col("l.amount").alias("excl"), F.col("l.amount_including_vat").alias("incl"))
    expected = raw.groupBy(*groups).agg(F.count("*").alias("rows"),
                                       F.sum("excl").alias("excl"), F.sum("incl").alias("incl"))
    if document_type == "CREDIT_MEMO":
        expected = expected.withColumn("excl", -F.col("excl")).withColumn("incl", -F.col("incl"))
    observed = actual.filter(F.col("document_type") == document_type).groupBy(*groups).agg(
        F.count("*").alias("rows"), F.sum("signed_amount_excl_vat").alias("excl"),
        F.sum("signed_amount_incl_vat").alias("incl"))
    if expected.exceptAll(observed).limit(1).count() or observed.exceptAll(expected).limit(1).count():
        raise ValueError(f"{document_type}: document counts or exact decimal totals differ from Silver")


# Exact decimal arithmetic avoids Spark's scale reduction when combining decimal(38,20).
def order_decimal_operation(left, right, operation):
    from decimal import Decimal, localcontext, ROUND_HALF_UP
    if left is None or right is None:
        return None
    with localcontext() as context:
        context.prec = 100
        value = left - right if operation == "subtract" else left * right
        value = value.quantize(Decimal("1e-20"), rounding=ROUND_HALF_UP)
        if abs(value) >= Decimal("1e18"):
            raise ValueError("Order calculation exceeds decimal(38,20)")
        return value


def remaining_quantity_exact(quantity, shipped):
    return order_decimal_operation(quantity, shipped, "subtract")


def open_value_exact(remaining, price):
    return order_decimal_operation(remaining, price, "multiply")


def order_candidates(lines):
    require_columns(lines, ["document_type", "document_no", "line_no", "type", "no",
                           "description", "quantity", "quantity_shipped", "quantity_invoiced",
                           "outstanding_quantity", "unit_price", "qty_shipped_not_invoiced",
                           "shipped_not_invoiced", "shipped_not_invoiced_lcy", "shipped_not_inv_lcy_no_vat",
                           "planned_shipment_date", "shipment_date", "unit_of_measure_code",
                           "bill_to_customer_no", "sell_to_customer_no", "currency_code", "_silver_run_id"],
                    "sales_line")
    df = lines.filter(F.col("document_type") == 1).withColumn("document_no", text_code(F.col("document_no")))
    validate_key(df, ["document_type", "document_no", "line_no"], "sales order lines")
    require_money(df, ["quantity", "quantity_shipped", "unit_price", "qty_shipped_not_invoiced"], "sales order lines")
    difference = F.udf(remaining_quantity_exact, DecimalType(38, 20))
    product = F.udf(open_value_exact, DecimalType(38, 20))
    df = df.withColumn("remaining_quantity", difference("quantity", "quantity_shipped"))
    # Physical filtering makes > 1 universal for every open-orders measure.
    return df.withColumn("open_order_value", product("remaining_quantity", "unit_price"))


def build_order_facts(sources):
    candidates = order_candidates(sources["sales_line"])
    relevant = candidates.filter((F.col("remaining_quantity") > F.lit(Decimal("1"))) |
                                 (F.col("qty_shipped_not_invoiced") > 0))
    require_columns(sources["sales_header"], ["document_type", "no", "order_date", "bill_to_customer_no",
                    "sell_to_customer_no", "salesperson_code", "currency_code", "currency_factor",
                    "prices_including_vat", "status", "_silver_run_id"], "sales_header")
    headers = (sources["sales_header"].filter(F.col("document_type") == 1)
               .withColumn("no", text_code(F.col("no"))))
    validate_key(headers, ["document_type", "no"], "sales order headers")
    header_keys = headers.select("document_type", F.col("no").alias("document_no"))
    if relevant.join(header_keys, ["document_type", "document_no"], "left_anti").limit(1).count():
        raise ValueError("Order line without matching document-type/order header")
    ext = sources["sales_line_ext"]
    po_column = "acr_customer_po_no_058edfa3_6aec_4ddd_95cd_a9cde631b170"
    production_column = "acr_production_order_no_058edfa3_6aec_4ddd_95cd_a9cde631b170"
    require_columns(ext, ["document_type", "document_no", "line_no", po_column, production_column, "_silver_run_id"],
                    "sales_line_ext")
    ext = ext.filter(F.col("document_type") == 1).withColumn("document_no", text_code(F.col("document_no")))
    validate_key(ext, ["document_type", "document_no", "line_no"], "sales line extension")
    j = relevant.alias("l").join(headers.alias("h"),
        (F.col("l.document_type") == F.col("h.document_type")) &
        (F.col("l.document_no") == F.col("h.no")), "inner")
    j = j.join(ext.alias("e"), (F.col("l.document_type") == F.col("e.document_type")) &
               (F.col("l.document_no") == F.col("e.document_no")) &
               (F.col("l.line_no") == F.col("e.line_no")), "left")
    reject_rows(j, F.col("l.type").isNull(), "Order: missing line type")
    defaulted = {}
    for c in ["bill_to_customer_no", "sell_to_customer_no"]:
        lk, hk = relation_key(F.col("l." + c)), relation_key(F.col("h." + c))
        defaulted[c] = (F.col("l.type") == 0) & (lk == "U:") & (hk != "U:")
        reject_rows(j, (~lk.eqNullSafe(hk)) & ~defaulted[c], "Order: line/header " + c + " differs")
    currency = lambda c: F.coalesce(text_code(c), F.lit(""))
    reject_rows(j, currency(F.col("l.currency_code")) != currency(F.col("h.currency_code")),
                "Order: line/header currency differs")
    raw_date = F.to_date(F.col("l.planned_shipment_date"))
    valid_date = raw_date.isNotNull() & F.year(raw_date).between(MIN_POSTING_YEAR, MAX_POSTING_YEAR)
    planned_date = F.when(valid_date, raw_date).otherwise(F.lit(None).cast("date"))
    base = j.select(
        F.lit(COMPANY_KEY).alias("company_key"), F.col("l.document_type"), F.col("l.document_no"), F.col("l.line_no"),
        F.to_json(F.struct(F.lit(COMPANY_KEY).alias("company"), F.col("l.document_type").alias("type"),
                          F.col("l.document_no").alias("number"))).alias("order_key"),
        F.col("l.type").alias("line_type"), F.col("l.no").alias("source_line_no"), F.col("l.description"),
        relation_key(F.col("h.bill_to_customer_no")).alias("bill_to_customer_key"),
        relation_key(F.col("h.sell_to_customer_no")).alias("sell_to_customer_key"),
        relation_key(F.col("h.salesperson_code")).alias("salesperson_key"),
        F.when(F.col("l.type") == ITEM_LINE_TYPE, relation_key(F.col("l.no"))).otherwise(F.lit("N:")).alias("item_key"),
        F.col("l.bill_to_customer_no").alias("source_line_bill_to_customer_no"),
        F.col("l.sell_to_customer_no").alias("source_line_sell_to_customer_no"),
        defaulted["bill_to_customer_no"].alias("bill_to_customer_defaulted"),
        defaulted["sell_to_customer_no"].alias("sell_to_customer_defaulted"),
        F.to_date(F.col("h.order_date")).alias("order_date"),
        F.to_date(F.col("l.shipment_date")).alias("source_shipment_date"),
        raw_date.alias("source_planned_shipment_date"), planned_date.alias("planned_shipment_date"),
        F.date_format(planned_date, "yyyyMMdd").cast("int").alias("planned_shipment_date_key"),
        (~valid_date).alias("planned_shipment_date_missing_or_invalid"),
        currency(F.col("h.currency_code")).alias("currency_code"), F.col("h.currency_factor"),
        F.col("h.prices_including_vat"), F.col("h.status").alias("order_status"),
        F.col("l.unit_of_measure_code"), F.col("l.quantity").alias("ordered_quantity"),
        F.col("l.quantity_shipped"), F.col("l.quantity_invoiced"), F.col("l.outstanding_quantity").alias("source_outstanding_quantity"),
        F.col("l.remaining_quantity"), F.col("l.unit_price"), F.col("l.open_order_value"),
        F.col("l.qty_shipped_not_invoiced").alias("shipped_not_invoiced_quantity"),
        F.col("l.shipped_not_invoiced").alias("source_shipped_not_invoiced_amount"),
        F.col("l.shipped_not_invoiced_lcy").alias("source_shipped_not_invoiced_amount_lcy"),
        F.col("l.shipped_not_inv_lcy_no_vat").alias("shipped_not_invoiced_excl_vat_lcy"),
        F.col("e." + po_column).alias("customer_po"), F.col("e." + production_column).alias("production_order_no"),
        F.col("e.document_no").isNull().alias("line_extension_missing"),
        F.col("l._silver_run_id").alias("_silver_line_run_id"),
        F.col("h._silver_run_id").alias("_silver_header_run_id"),
        F.col("e._silver_run_id").alias("_silver_line_ext_run_id"),
    )
    common_exclusions = ["open_order_value", "shipped_not_invoiced_quantity",
                         "source_shipped_not_invoiced_amount", "source_shipped_not_invoiced_amount_lcy",
                         "shipped_not_invoiced_excl_vat_lcy"]
    common = [c for c in base.columns if c not in common_exclusions]
    open_orders = base.filter(F.col("remaining_quantity") > F.lit(Decimal("1"))).select(*common, "open_order_value")
    shipped = base.filter(F.col("shipped_not_invoiced_quantity") > 0).select(
        *common, "shipped_not_invoiced_quantity", "source_shipped_not_invoiced_amount",
        "source_shipped_not_invoiced_amount_lcy", "shipped_not_invoiced_excl_vat_lcy")
    require_money(shipped, ["shipped_not_invoiced_quantity", "source_shipped_not_invoiced_amount",
                           "source_shipped_not_invoiced_amount_lcy", "shipped_not_invoiced_excl_vat_lcy"], "shipped not invoiced")
    for name, df in [("fact_open_sales_orders", open_orders), ("fact_shipped_not_invoiced", shipped)]:
        validate_key(df, OUTPUT_KEYS[name], name)
    reconcile_orders(candidates, open_orders, shipped)
    return {"fact_open_sales_orders": open_orders, "fact_shipped_not_invoiced": shipped}


def reconcile_orders(candidates, open_orders, shipped):
    keys = ["document_type", "document_no", "line_no"]
    def same(expected, actual, label):
        if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
            raise ValueError(label + ": selected keys, quantities or values differ from Silver")
    expected_open = candidates.filter(F.col("remaining_quantity") > F.lit(Decimal("1")))
    columns = keys + ["remaining_quantity", "unit_price", "open_order_value"]
    same(expected_open.select(*columns), open_orders.select(*columns), "Open orders")
    expected_shipped = candidates.filter(F.col("qty_shipped_not_invoiced") > 0).select(
        *keys, F.col("qty_shipped_not_invoiced").alias("shipped_not_invoiced_quantity"),
        F.col("shipped_not_invoiced").alias("source_shipped_not_invoiced_amount"),
        F.col("shipped_not_invoiced_lcy").alias("source_shipped_not_invoiced_amount_lcy"),
        F.col("shipped_not_inv_lcy_no_vat").alias("shipped_not_invoiced_excl_vat_lcy"))
    same(expected_shipped, shipped.select(*expected_shipped.columns), "Shipped not invoiced")


def check_order_relationships(outputs):
    for name in ["fact_open_sales_orders", "fact_shipped_not_invoiced"]:
        fact = outputs[name]
        links = [("bill_to_customer_key", "dim_customer", "customer_key"),
                 ("sell_to_customer_key", "dim_customer", "customer_key"),
                 ("item_key", "dim_item", "item_key"),
                 ("salesperson_key", "dim_salesperson", "salesperson_key"),
                 ("planned_shipment_date_key", "dim_date", "date_key")]
        for fk, dim, pk in links:
            keys = fact.select(fk)
            if fk == "planned_shipment_date_key":
                keys = keys.filter(F.col(fk).isNotNull())
            if keys.distinct().join(outputs[dim].select(F.col(pk).alias(fk)), fk, "left_anti").limit(1).count():
                raise ValueError(f"{name}: unresolved {fk} -> {dim}.{pk}")


def print_order_summary(outputs):
    for name, qty, value in [
        ("fact_open_sales_orders", "remaining_quantity", "open_order_value"),
        ("fact_shipped_not_invoiced", "shipped_not_invoiced_quantity", "source_shipped_not_invoiced_amount"),
    ]:
        fact = outputs[name]
        print(name + ": totals by document currency and UOM; no currency conversion")
        (fact.groupBy("currency_code", "unit_of_measure_code", "prices_including_vat")
         .agg(F.count("*").alias("lines"), F.countDistinct("order_key").alias("orders"),
              F.sum(qty).alias("quantity"), F.sum(value).alias("value"))
         .orderBy("currency_code", "unit_of_measure_code").show(100, truncate=False))
        print("Missing/invalid planned dates:", fact.filter("planned_shipment_date_missing_or_invalid").count())
        print("Missing line extensions:", fact.filter("line_extension_missing").count())


def build_gold(sources):
    invoice = build_document(sources["sales_invoice_line"], sources["sales_invoice_header"], "INVOICE")
    credit = build_document(sources["sales_credit_memo_line"], sources["sales_credit_memo_header"], "CREDIT_MEMO")
    fact = invoice.unionByName(credit).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        fact.count()
        validate_key(fact, OUTPUT_KEYS["fact_posted_sales"], "fact_posted_sales")
        for prefix, kind in [("sales_invoice", "INVOICE"), ("sales_credit_memo", "CREDIT_MEMO")]:
            reconcile_document(sources[prefix + "_line"], sources[prefix + "_header"], fact, kind)
        orders = build_order_facts(sources)
        # Shared dimensions cover all three facts, including order-only customers/items.
        reference_columns = ["bill_to_customer_key", "sell_to_customer_key", "item_key", "salesperson_key"]
        references = fact.select(*reference_columns)
        dates = fact.select("posting_date")
        for order_fact in orders.values():
            references = references.unionByName(order_fact.select(*reference_columns))
            dates = dates.unionByName(order_fact.select(F.col("planned_shipment_date").alias("posting_date")))
        outputs = build_dimensions(sources, references)
        outputs["dim_date"] = build_dates(dates)
        outputs["fact_posted_sales"] = fact
        outputs.update(orders)
        for name, frame in outputs.items():
            validate_key(frame, OUTPUT_KEYS[name], name)
        check_relationships(outputs)
        check_order_relationships(outputs)
        return outputs
    except Exception:
        fact.unpersist()
        raise


def current_version(table):
    return int(spark.sql(f"DESCRIBE HISTORY {table}").select("version").first()["version"])


def load_sources():
    audit_name = f"{SOURCE_LAKEHOUSE}.{SOURCE_SCHEMA}._load_audit"
    audit = spark.table(audit_name)
    sources, manifest = {}, []
    for name in SOURCE_TABLES:
        table = f"{SOURCE_LAKEHOUSE}.{SOURCE_SCHEMA}.{name}"
        # Pin each Delta version to make retries/recomputation within this run stable.
        detail = spark.sql(f"DESCRIBE DETAIL {table}").first()
        if detail["format"].lower() != "delta":
            raise ValueError(f"{table}: expected a persisted Silver Delta table")
        version = current_version(table)
        df = spark.read.format("delta").option("versionAsOf", version).load(detail["location"])
        require_columns(df, ["_silver_run_id"], table)
        latest = (audit.filter(F.col("target_table") == table)
                  .orderBy(F.col("completed_at").desc()).limit(1).collect())
        if not latest or latest[0]["status"] != "SUCCESS":
            raise ValueError(f"{table}: latest Silver audit is missing or unsuccessful")
        run = latest[0]
        if run["effective_mode"] not in {"full", "bootstrap_full"}:
            raise ValueError(f"{table}: Gold requires audited full Silver snapshots")
        reject_rows(df, F.col("_silver_run_id").isNull() | (F.col("_silver_run_id") != run["run_id"]),
                    f"{table}: rows do not match latest audited full load")
        count = df.count()
        if run["saved_rows"] != count:
            raise ValueError(f"{table}: audit row count differs from pinned source")
        sources[name] = df
        manifest.append({"table": table, "version": version, "rows": count,
                         "silver_run_id": run["run_id"], "silver_batch_id": run["batch_id"]})
    return sources, manifest


def assert_sources_unchanged(manifest):
    for entry in manifest:
        if current_version(entry["table"]) != entry["version"]:
            raise ValueError("Silver changed during Gold preparation; run without overlapping writers")


def schema_signature(frame):
    return {field.name: field.dataType.simpleString() for field in frame.schema.fields}


def run_gold():
    import re
    for value in [SOURCE_LAKEHOUSE, SOURCE_SCHEMA, TARGET_LAKEHOUSE, TARGET_SCHEMA]:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Use simple lakehouse/schema names")
    if SOURCE_LAKEHOUSE == TARGET_LAKEHOUSE:
        raise ValueError("Use a separate Gold lakehouse")
    configure_spark()
    run_id = str(uuid4())
    started = datetime.now(timezone.utc).isoformat()
    print("Gold run:", run_id, "| WRITE_GOLD:", WRITE_GOLD)
    sources, manifest = load_sources()
    print("Pinned Silver sources:")
    for row in manifest:
        print(row["table"], "version", row["version"], "rows", row["rows"])
    outputs = build_gold(sources)
    raw_fact = outputs["fact_posted_sales"]
    cached = []
    try:
        counts = {}
        built_at = datetime.now(timezone.utc)
        for name, frame in list(outputs.items()):
            out = frame.withColumn("_gold_run_id", F.lit(run_id)).withColumn("_gold_processed_at", F.lit(built_at))
            out = out.persist(StorageLevel.MEMORY_AND_DISK)
            cached.append(out)
            counts[name] = out.count()
            outputs[name] = out
            print(f"VALIDATED: {name} | {counts[name]:,} rows")
        print("Totals in document currency (blank currency_code means the company's local currency):")
        (outputs["fact_posted_sales"].groupBy("document_type", "currency_code").agg(
            F.count("*").alias("rows"), F.sum("signed_amount_excl_vat").alias("signed_excl_vat"),
            F.sum("signed_amount_incl_vat").alias("signed_incl_vat"))
         .orderBy("document_type", "currency_code").show(truncate=False))
        for table in ["dim_customer", "dim_item", "dim_salesperson"]:
            print(table, "placeholder members:", outputs[table].filter("is_placeholder").count())
        print("Items with unresolved categories:", outputs["dim_item"].filter("category_missing").count())
        print("Comment lines using header date:", outputs["fact_posted_sales"].filter(
            "line_posting_date_defaulted").count())
        for customer_field in ["bill_to_customer", "sell_to_customer"]:
            print(f"Comment lines using header {customer_field}:",
                  outputs["fact_posted_sales"].filter(customer_field + "_defaulted").count())
        print("Credit rows with negative source amount:", outputs["fact_posted_sales"].filter(
            (F.col("document_type") == "CREDIT_MEMO") & (F.col("source_amount_excl_vat") < 0)).count())
        print_order_summary(outputs)
        assert_sources_unchanged(manifest)
        if not WRITE_GOLD:
            print("VALIDATION COMPLETE: seven proposed Gold tables passed checks. No Gold tables were written.")
            return
        target_prefix = f"{TARGET_LAKEHOUSE}.{TARGET_SCHEMA}"
        # Target lakehouse must already exist and be accessible/attached in Fabric.
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_prefix}")
        for name, frame in outputs.items():
            target = f"{target_prefix}.{name}"
            if spark.catalog.tableExists(target) and schema_signature(spark.table(target)) != schema_signature(frame):
                raise ValueError(f"{target}: schema drift; review before replacing any Gold table")
        audit_schema = "run_id string, status string, started_at string, completed_at string, source_manifest string, output_counts string, written_tables string, error string"
        audit_table = target_prefix + "._gold_load_audit"
        if not spark.catalog.tableExists(audit_table):
            spark.createDataFrame([], audit_schema).write.format("delta").mode("errorifexists").saveAsTable(audit_table)
        if schema_signature(spark.table(audit_table)) != schema_signature(spark.createDataFrame([], audit_schema)):
            raise ValueError("Gold audit schema differs; resolve before writing")
        def audit(status, written, error=""):
            row = (run_id, status, started, datetime.now(timezone.utc).isoformat(),
                   json.dumps(manifest), json.dumps(counts), json.dumps(written), error[:4000])
            spark.createDataFrame([row], audit_schema).write.format("delta").mode("append").saveAsTable(audit_table)
        written = []
        audit("RUNNING", written)
        try:
            for name in OUTPUT_KEYS:  # Dimensions first, fact last.
                target = f"{target_prefix}.{name}"
                mode = "overwrite" if spark.catalog.tableExists(target) else "errorifexists"
                outputs[name].write.format("delta").mode(mode).saveAsTable(target)
                written.append(name)
                saved = spark.table(target).select(*outputs[name].columns)
                # Exact multiset comparison checks values, duplicates, row counts and lineage.
                if saved.exceptAll(outputs[name]).limit(1).count() or outputs[name].exceptAll(saved).limit(1).count():
                    raise ValueError(f"{target}: saved contents differ from validated data")
                print("SAVED AND VERIFIED:", target)
            audit("SUCCESS", written)
        except Exception as error:
            try:
                audit("FAILED", written, str(error))
            except Exception:
                print("Could not record failure; the RUNNING audit may remain. Inspect pipeline logs.")
            raise
        print("COMPLETE: all seven Gold tables written, verified, and audited.")
    finally:
        for frame in cached:
            frame.unpersist()
        raw_fact.unpersist()
