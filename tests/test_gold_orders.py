"""Open-order boundary, enrichment and shared-dimension tests for the maintained Gold notebook."""
from datetime import datetime, date
from decimal import Decimal, localcontext, ROUND_HALF_UP
from pathlib import Path
from types import SimpleNamespace
import json
import pytest
from pyspark.sql import SparkSession, functions as F
from test_gold_notebook import sources as posted_sources


@pytest.fixture(scope='module')
def ctx(tmp_path_factory):
    spark=(SparkSession.builder.master('local[2]').appName('gold-order-tests')
           .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
           .config('spark.sql.warehouse.dir',str(tmp_path_factory.mktemp('warehouse-orders')))
           .config('spark.driver.bindAddress','127.0.0.1').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    n=json.loads((Path(__file__).parents[1]/'notebooks/Silver_to_Gold_v1_Posted_Sales.ipynb').read_text())
    ns={'spark':spark}
    for c in n['cells']:
        if c['cell_type']=='code' and 'definitions' in c['metadata'].get('tags',[]):
            exec(compile(''.join(c['source']),'gold-cell','exec'),ns)
    ns['configure_spark']()
    yield SimpleNamespace(spark=spark,ns=ns)
    spark.stop()


def sources(ctx):
    s=posted_sources(ctx)
    hs=('document_type int, no string, order_date timestamp, bill_to_customer_no string, '
        'sell_to_customer_no string, salesperson_code string, currency_code string, '
        'currency_factor decimal(38,20), prices_including_vat int, status int, _silver_run_id string')
    def h(kind,doc,cust,rep):return(kind,doc,datetime(2026,10,1),cust,cust,rep,'USD',Decimal('1'),0,1,'s1')
    s['sales_header']=ctx.spark.createDataFrame([
        h(1,'ORD1','C1','R1'),h(1,'ORD2','CORDER','RORDER'),h(0,'ORD1','QUOTE','QREP'),h(5,'ORD1','RETURN','RREP')],hs)
    ls=('document_type int, document_no string, line_no int, type int, no string, description string, '
        'quantity decimal(38,20), quantity_shipped decimal(38,20), quantity_invoiced decimal(38,20), '
        'outstanding_quantity decimal(38,20), unit_price decimal(38,20), qty_shipped_not_invoiced decimal(38,20), '
        'shipped_not_invoiced decimal(38,20), shipped_not_invoiced_lcy decimal(38,20), shipped_not_inv_lcy_no_vat decimal(38,20), '
        'planned_shipment_date timestamp, shipment_date timestamp, unit_of_measure_code string, '
        'bill_to_customer_no string, sell_to_customer_no string, currency_code string, _silver_run_id string')
    def line(num,q,shipped,price,sni='0',amount='0',doc='ORD1',kind=1,planned=datetime(2026,10,10)):
        cust,item=('C1','I1') if doc=='ORD1' else ('CORDER','IORDER')
        q,shipped,price,sni,amount=map(Decimal,[q,shipped,price,sni,amount])
        return(kind,doc,num,2,item,'Synthetic',q,shipped,shipped-sni,q-shipped,price,sni,amount,amount,amount,
               planned,datetime(2026,10,10),'EA',cust,cust,'USD','s1')
    rows=[line(1,'10','3','12.5','2','25'),line(2,'5','4','5','4','20'),
          line(3,'5.5','4','10'),line(4,'2.5','2','10'),line(5,'1','2','10'),
          line(6,'3','3','10','3','30'),
          line(1,'2','0','3',doc='ORD2',planned=datetime(2028,1,5)),
          line(2,'1.00000000000000000001','0','2',doc='ORD2',planned=None),
          line(1,'999','0','99',kind=0),line(1,'999','0','99',kind=5)]
    s['sales_line']=ctx.spark.createDataFrame(rows,ls)
    es=('document_type int, document_no string, line_no int, '
        'acr_customer_po_no_058edfa3_6aec_4ddd_95cd_a9cde631b170 string, '
        'acr_production_order_no_058edfa3_6aec_4ddd_95cd_a9cde631b170 string, _silver_run_id string')
    s['sales_line_ext']=ctx.spark.createDataFrame([(1,'ORD1',1,'PO1','PROD1','s1'),(0,'ORD1',1,'BAD_QUOTE','BAD_QUOTE','s1')],es)
    return s


def test_all_open_measures_share_threshold_and_sni_is_separate(ctx):
    out=ctx.ns['build_order_facts'](sources(ctx))
    rows=out['fact_open_sales_orders'].collect()
    assert {(r.document_no,r.line_no) for r in rows}=={('ORD1',1),('ORD1',3),('ORD2',1),('ORD2',2)}
    assert all(r.remaining_quantity>Decimal('1') and r.document_type==1 for r in rows)
    assert sum(r.open_order_value for r in rows)==Decimal('110.50000000000000000002')
    assert len({r.order_key for r in rows})==2
    assert sum(r.remaining_quantity for r in rows)==Decimal('11.50000000000000000001')
    enriched=next(r for r in rows if r.document_no=='ORD1' and r.line_no==1)
    assert enriched.customer_po=='PO1' and enriched.production_order_no=='PROD1'
    assert not enriched.line_extension_missing
    unknown=next(r for r in rows if r.document_no=='ORD2' and r.line_no==2)
    assert unknown.planned_shipment_date_key is None and unknown.planned_shipment_date_missing_or_invalid
    assert unknown.line_extension_missing
    shipped=out['fact_shipped_not_invoiced'].collect()
    assert {r.line_no for r in shipped}=={1,2,6}
    assert sum(r.shipped_not_invoiced_quantity for r in shipped)==Decimal('9')
    assert sum(r.source_shipped_not_invoiced_amount for r in shipped)==Decimal('75')
    assert next(r for r in shipped if r.line_no==6).remaining_quantity==Decimal('0')


def test_complete_gold_covers_order_only_dimensions_and_future_dates(ctx):
    out=ctx.ns['build_gold'](sources(ctx))
    try:
        assert len(out)==7 and out['fact_posted_sales'].count()==5
        assert out['dim_customer'].filter("customer_key='K:CORDER' AND is_placeholder").count()==1
        assert out['dim_item'].filter("item_key='K:IORDER' AND is_placeholder").count()==1
        assert out['dim_salesperson'].filter("salesperson_key='K:RORDER' AND is_placeholder").count()==1
        assert out['dim_date'].filter("date = DATE '2028-01-05'").count()==1
    finally:out['fact_posted_sales'].unpersist()


@pytest.mark.parametrize('issue,message',[
    ('header','without matching'),('duplicate_extension','duplicate key'),
    ('duplicate_line','duplicate key'),('null_price','null numeric value'),
    ('customer','bill_to_customer_no differs'),('currency','currency differs'),
])
def test_bad_order_sources_fail(ctx,issue,message):
    s=sources(ctx)
    if issue=='header':s['sales_header']=s['sales_header'].filter("no <> 'ORD2'")
    if issue=='duplicate_extension':s['sales_line_ext']=s['sales_line_ext'].unionByName(s['sales_line_ext'].filter('document_type=1'))
    if issue=='duplicate_line':s['sales_line']=s['sales_line'].unionByName(s['sales_line'].filter("document_type=1 AND document_no='ORD2' AND line_no=1"))
    if issue=='null_price':s['sales_line']=s['sales_line'].withColumn('unit_price',F.lit(None).cast('decimal(38,20)'))
    if issue=='customer':s['sales_line']=s['sales_line'].withColumn('bill_to_customer_no',F.lit('WRONG'))
    if issue=='currency':s['sales_line']=s['sales_line'].withColumn('currency_code',F.lit('EUR'))
    with pytest.raises(ValueError,match=message):ctx.ns['build_order_facts'](s)


def test_empty_backlog_is_valid_and_clears_old_orders(ctx):
    s=sources(ctx)
    s['sales_line']=s['sales_line'].withColumn('quantity_shipped',F.col('quantity')).withColumn('qty_shipped_not_invoiced',F.lit(Decimal('0')).cast('decimal(38,20)'))
    out=ctx.ns['build_order_facts'](s)
    assert out['fact_open_sales_orders'].count()==0
    assert out['fact_shipped_not_invoiced'].count()==0


def test_decimal_arithmetic_preserves_threshold_and_price_precision(ctx):
    diff=ctx.ns['remaining_quantity_exact']
    price=ctx.ns['open_value_exact']
    assert diff(Decimal('2.00000000000000000001'),Decimal('1'))>Decimal('1')
    a,b=Decimal('1.12345678901234567890'),Decimal('3.12345678901234567890')
    with localcontext() as c:
        c.prec=100
        expected=(a*b).quantize(Decimal('1e-20'),rounding=ROUND_HALF_UP)
    assert price(a,b)==expected
    assert price(Decimal('2'),Decimal('-3'))==Decimal('-6')
