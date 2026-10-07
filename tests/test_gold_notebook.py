"""Synthetic Spark tests. Run with pytest and PySpark 3.5; no Fabric access required."""
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
import json
import pytest
from pyspark.sql import SparkSession, functions as F


@pytest.fixture(scope='module')
def ctx(tmp_path_factory):
    spark = (SparkSession.builder.master('local[2]').appName('gold-notebook-tests')
             .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
             .config('spark.sql.warehouse.dir',str(tmp_path_factory.mktemp('warehouse')))
             .config('spark.driver.bindAddress','127.0.0.1').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    n=json.loads((Path(__file__).parents[1]/'notebooks/Silver_to_Gold_v1_Posted_Sales.ipynb').read_text())
    ns={'spark':spark}
    for c in n['cells']:
        if c['cell_type']=='code' and 'definitions' in c['metadata'].get('tags',[]):
            exec(compile(''.join(c['source']),'gold-cell','exec'),ns)
    ns['configure_spark']()
    yield SimpleNamespace(spark=spark, ns=ns)
    spark.stop()


def sources(ctx):
    spark=ctx.spark
    stamp=datetime(2024,2,29)
    hs=('no string, posting_date timestamp, document_date timestamp, bill_to_customer_no string, '
        'sell_to_customer_no string, salesperson_code string, currency_code string, currency_factor decimal(38,20), '
        'correction int, marked_as_canceled int, prepayment_invoice int, prepayment_credit_memo int, _silver_run_id string')
    ls=('document_no string, line_no int, type int, no string, description string, quantity decimal(38,20), '
        'quantity_base decimal(38,20), unit_of_measure_code string, amount decimal(38,20), '
        'amount_including_vat decimal(38,20), posting_date timestamp, bill_to_customer_no string, '
        'sell_to_customer_no string, _silver_run_id string')
    def h(doc,cust,rep,cur):return (doc,stamp,stamp,cust,cust,rep,cur,Decimal('1'),0,0,0,0,'s1')
    def l(doc,num,kind,item,amt,vat,cust):return (doc,num,kind,item,'Synthetic',Decimal('2'),Decimal('2'),'EA',Decimal(amt),Decimal(vat),stamp,cust,cust,'s1')
    return {
        'sales_invoice_header':spark.createDataFrame([h('A','C1','R1','USD'),h('B','C_MISSING','','')],hs),
        'sales_invoice_line':spark.createDataFrame([l('A',1,2,'I1','100','110','C1'),l('A',2,1,'GL1','25','27.5','C1'),l('B',1,2,'I_MISSING','-10','-11','C_MISSING')],ls),
        'sales_credit_memo_header':spark.createDataFrame([h('A','C1','R1','USD')],hs),
        'sales_credit_memo_line':spark.createDataFrame([l('A',1,2,'I1','20','22','C1'),l('A',2,2,'I1','-5','-5.5','C1')],ls),
        'customer':spark.createDataFrame([('C1','Customer','City','County','00001','US','R1','GROUP',0)],'no string, name string, city string, county string, post_code string, country_region_code string, salesperson_code string, customer_posting_group string, blocked int'),
        'item':spark.createDataFrame([('I1','Item','Other','EA','CAT',Decimal('1'),Decimal('2'),0)],'no string, description string, description_2 string, base_unit_of_measure string, item_category_code string, net_weight decimal(38,20), gross_weight decimal(38,20), blocked int'),
        'item_category':spark.createDataFrame([('CAT','Category','')],'code string, description string, parent_category string'),
        'salesperson_purchaser':spark.createDataFrame([('R1','Rep',0)],'code string, name string, blocked int'),
    }


def test_complete_gold_reconciles_and_preserves_keys(ctx):
    out=ctx.ns['build_gold'](sources(ctx))
    try:
        fact=out['fact_posted_sales']
        assert fact.count()==5
        assert fact.select('document_type','document_no','line_no').distinct().count()==5
        credits={r.line_no:r.signed_amount_excl_vat for r in fact.filter("document_type = 'CREDIT_MEMO'").collect()}
        assert credits=={1:Decimal('-20'),2:Decimal('5')}
        totals={r.currency_code:r.total for r in fact.groupBy('currency_code').agg(F.sum('signed_amount_excl_vat').alias('total')).collect()}
        assert totals=={'USD':Decimal('110'),'':Decimal('-10')}
        assert out['dim_customer'].filter("customer_key = 'K:C_MISSING' AND is_placeholder").count()==1
        assert out['dim_item'].filter("item_key = 'N:' AND is_placeholder").count()==1
        assert out['dim_item'].filter("item_key = 'K:I_MISSING' AND is_placeholder").count()==1
        assert out['dim_salesperson'].filter("salesperson_key = 'U:' AND is_placeholder").count()==1
        assert out['dim_date'].filter("date = DATE '2024-02-29'").count()==1
    finally:out['fact_posted_sales'].unpersist()


@pytest.mark.parametrize('issue,message',[
    ('duplicate_header','duplicate key'),('orphan','without matching header'),
    ('date','posting dates differ'),('customer','bill_to_customer_no differs'),
    ('null_amount','null numeric value'),('duplicate_line','duplicate key')])
def test_bad_sales_fail_before_publication(ctx,issue,message):
    s=sources(ctx); l=s['sales_invoice_line']; h=s['sales_invoice_header']
    if issue=='duplicate_header': h=h.unionByName(h.limit(1))
    if issue=='duplicate_line': l=l.unionByName(l.limit(1))
    if issue=='orphan': h=h.filter("no <> 'B'")
    if issue=='date': l=l.withColumn('posting_date',F.lit(datetime(2024,3,1)))
    if issue=='customer': l=l.withColumn('bill_to_customer_no',F.lit('WRONG'))
    if issue=='null_amount': l=l.withColumn('amount',F.lit(None).cast('decimal(38,20)'))
    with pytest.raises(ValueError,match=message): ctx.ns['build_document'](l,h,'INVOICE')


def test_reconciliation_catches_amount_corruption(ctx):
    s=sources(ctx);l=s['sales_invoice_line'];h=s['sales_invoice_header']
    fact=ctx.ns['build_document'](l,h,'INVOICE').withColumn('signed_amount_excl_vat',F.lit(Decimal('999')).cast('decimal(38,20)'))
    with pytest.raises(ValueError,match='totals differ'):
        ctx.ns['reconcile_document'](l,h,fact,'INVOICE')


def test_schema_does_not_silently_accept_missing_money(ctx):
    s=sources(ctx)
    with pytest.raises(ValueError,match='missing columns'):
        ctx.ns['build_document'](s['sales_invoice_line'].drop('amount'),s['sales_invoice_header'],'INVOICE')


def test_decimal_negation_does_not_round_to_six_places(ctx):
    s=sources(ctx)
    value=Decimal('12.12345678901234567890')
    l=s['sales_credit_memo_line'].limit(1).withColumn('amount',F.lit(value).cast('decimal(38,20)'))
    out=ctx.ns['build_document'](l,s['sales_credit_memo_header'],'CREDIT_MEMO')
    assert out.first().signed_amount_excl_vat==-value
    assert out.schema['signed_amount_excl_vat'].dataType.scale==20


def test_comment_default_date_uses_header_and_preserves_lineage(ctx):
    s=sources(ctx)
    lines=(s['sales_invoice_line'].filter("document_no = 'A' AND line_no = 1")
           .withColumn('type',F.lit(0))
           .withColumn('posting_date',F.lit(datetime(1753,1,1)))
           .withColumn('amount',F.lit(Decimal('0')).cast('decimal(38,20)')))
    fact=ctx.ns['build_document'](lines,s['sales_invoice_header'],'INVOICE')
    row=fact.first()
    assert row.posting_date==date(2024,2,29)
    assert row.source_line_posting_date==date(1753,1,1)
    assert row.line_posting_date_defaulted is True
    assert row.is_financial_line is False
    ctx.ns['reconcile_document'](lines,s['sales_invoice_header'],fact,'INVOICE')


@pytest.mark.parametrize('line_type,line_date',[
    (2,datetime(1753,1,1)),  # Financial line cannot use the comment exception.
    (0,datetime(2024,3,1)), # Ordinary conflicting comment dates still fail.
    (0,None),             # An unexplained null is not the observed sentinel.
])
def test_comment_date_exception_remains_narrow(ctx,line_type,line_date):
    s=sources(ctx)
    lines=(s['sales_invoice_line'].limit(1)
           .withColumn('type',F.lit(line_type))
           .withColumn('posting_date',F.lit(line_date).cast('timestamp')))
    with pytest.raises(ValueError,match='posting dates differ'):
        ctx.ns['build_document'](lines,s['sales_invoice_header'],'INVOICE')
