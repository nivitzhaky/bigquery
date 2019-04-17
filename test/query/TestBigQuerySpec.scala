package query

import algo.{QRRow, QueryBuilder, QueryResult}
import org.specs2.matcher.{MatcherMacros, MustMatchers, ResultMatchers}
import org.specs2.mutable.Specification

class TestBigQuerySpec extends Specification with ResultMatchers with MatcherMacros with MustMatchers {

  "BigQuery" should {
    "Count orders " in new TestContext {
      new QueryBuilder(meta).withField("orderid").doQuery() must_=== QueryResult(List(QRRow(Map("orderid" -> "5009"))))
    }
    "Count customers " in new TestContext {
      new QueryBuilder(meta).withField("customer").doQuery() must_=== QueryResult(List(QRRow(Map("customer" -> "793"))))
    }
    "Select category" in new TestContext {
      new QueryBuilder(meta).withField("productcategory").doQuery() must_=== QueryResult(List(
        QRRow(Map("productcategory" -> "Office Supplies")),
        QRRow(Map("productcategory" -> "Furniture")),
        QRRow(Map("productcategory" -> "Technology"))
        ))
    }
    "Select category customer" in new TestContext {
      new QueryBuilder(meta).withField("productcategory").withField("customer")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("productcategory" -> "Office Supplies", "customer"-> "788" )),
        QRRow(Map("productcategory" -> "Furniture", "customer"-> "707")),
        QRRow(Map("productcategory" -> "Technology", "customer"-> "687"))
      )
    }

    "Select category customer with condition" in new TestContext {
      new QueryBuilder(meta).withField("customer").withField("productcategory")
        .withCondition("numpurchases", ">", "2")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("productcategory" -> "Office Supplies", "customer"-> "673" )),
        QRRow(Map("productcategory" -> "Furniture", "customer"-> "297")),
        QRRow(Map("productcategory" -> "Technology", "customer"-> "237"))
      )
    }

    "Select category customer with sum purchases" in new TestContext {
      new QueryBuilder(meta).withField("customer").withField("sumpurchases")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("sumpurchases" -> "2297200.8603000012", "customer"-> "793" ))
      )
    }

    "Select category customer with sum purchases and product" in new TestContext {
      new QueryBuilder(meta).withField("customer").withField("sumpurchases")
        .withCondition("productcategory", "=", "'Office Supplies'")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("sumpurchases" -> "20935133.469299879", "customer"-> "788" ))
      )
    }

    "Select with product and report" in new TestContext {
      new QueryBuilder(meta).withField("productcategory").
        withReport("default").doQuery().data.toSet must_=== Set(
        QRRow(Map("productcategory" -> "Office Supplies", "orderid" -> "3742", "customer" -> "788", "avgpurchase" -> "192.15580758952456",
          "productcount" -> "1058", "avgdiscount" -> "0.15728509790906062")),
        QRRow(Map("productcategory" -> "Furniture", "orderid" -> "1764", "customer" -> "707", "avgpurchase" -> "420.63480459183688",
          "productcount" -> "380", "avgdiscount" -> "0.17392267798208341")),
        QRRow(Map("productcategory" -> "Technology", "orderid" -> "1544", "customer" -> "687", "avgpurchase" -> "541.55053950776937",
          "productcount" -> "412", "avgdiscount" -> "0.13232268543584197")))

    }

    "Select subcategory size" in new TestContext {
      new QueryBuilder(meta).withField("productsubcategory").doQuery().data.size must_=== 17
    }

    "Select subcategory size" in new TestContext {
      new QueryBuilder(meta).withField("productsubcategory").doQuery().data.size must_=== 17
    }

    "Select sumpurchase group" in new TestContext {
      new QueryBuilder(meta).withCondition("paid", ">", "1000").
        saveAsGroup("group1","group1")

      new QueryBuilder(meta).withField("customer").withCondition("group_id", "=", "'group1'").
        doQuery().data.toSet must_=== Set(
        QRRow(Map("customer"-> "626" ))
      )
    }

    "Select sumpurchase not in group" in new TestContext {
      new QueryBuilder(meta).withCondition("paid", ">", "1000").
        saveAsGroup("group1","group1")

      new QueryBuilder(meta).withField("customer").
        withCondition("group_id", "!=", "'group1'").
        doQuery().data.toSet must_=== Set(
        QRRow(Map("customer"-> "167" ))
      )

    }
    "make date report daily" in new TestContext {
      new QueryBuilder(meta).makeDateReport("orderdate",Some("'2017-11-02'"),Some("'2017-11-03'"),"default","daily").
        data.toSet must_=== Set(
        QRRow(Map("orderdate" -> "2017-11-02", "orderid" -> "12", "customer" -> "12", "avgpurchase" -> "524.53216666666674",
          "productcount" -> "24", "avgdiscount" -> "0.076")),
        QRRow(Map("orderdate" -> "2017-11-03", "orderid" -> "16", "customer" -> "16", "avgpurchase" -> "283.55856250000005",
          "productcount" -> "23", "avgdiscount" -> "0.15652173913043477")))

    }

    "make date report weekly" in new TestContext {
      new QueryBuilder(meta).makeDateReport("orderdate",Some("'2017-11-02'"),Some("'2017-11-15'"),"default","weekly").
        data.toSet must_=== Set(
        QRRow(Map("orderdate" -> "2017-44", "orderid" -> "36", "customer" -> "36", "avgpurchase" -> "597.20608333333337", "productcount" -> "64",
          "avgdiscount" -> "0.17692307692307691")),
        QRRow(Map("orderdate" -> "2017-45", "orderid" -> "58", "customer" -> "55", "avgpurchase" -> "345.09568965517229", "productcount" -> "93",
          "avgdiscount" -> "0.15555555555555556")),
        QRRow(Map("orderdate" -> "2017-46", "orderid" -> "37", "customer" -> "36", "avgpurchase" -> "295.63957297297293", "productcount" -> "63",
          "avgdiscount" -> "0.20656249999999995"))
      )
    }

    "make date report monthly" in new TestContext {
      new QueryBuilder(meta).makeDateReport("orderdate",Some("'2017-11-01'"),Some("'2017-11-30'"),"default","monthly").
        data.toSet must_=== Set(
        QRRow(Map("orderdate" -> "2017-11", "orderid" -> "261", "customer" -> "216", "avgpurchase" -> "453.82308429118763",
          "productcount" -> "395", "avgdiscount" -> "0.16098039215686288"))
      )
    }
    "make date report" in new TestContext {
      new QueryBuilder(meta).makeDateReport("orderdate",Some("'2016-11-01'"),None,"default","yearly").
        data.toSet must_=== Set(
        QRRow(Map("orderdate" -> "2016", "orderid" -> "359", "customer" -> "289", "avgpurchase"-> "491.39556768802271", "productcount" -> "593",
          "avgdiscount" -> "0.14680055401662068")),
        QRRow(Map("orderdate" -> "2017", "orderid" -> "1687", "customer" -> "693", "avgpurchase" -> "434.62670729104838", "productcount" -> "1511",
          "avgdiscount" -> "0.15646739130434756"))
      )
    }

    "make property report" in new TestContext {
      new QueryBuilder(meta).makeProprtyStepReport("sumpurchases",0,4000,1000).
        data.toList must_=== List(
        QRRow(Map("count" -> "167", "step" -> "0 to 999", "avg" -> "552.89301676646687", "lower" -> "0.0",
          "stddev" -> "296.89287246958725", "sum" -> "92333.133799999981")),
        QRRow(Map("count" -> "186", "step" -> "1000 to 1999", "avg" -> "1456.1203715053766", "lower" -> "1000.0",
          "stddev" -> "281.93124951435834", "sum" -> "270838.38910000003")),
        QRRow(Map("count" -> "161", "step" -> "2000 to 2999", "avg" -> "2494.971412422361", "lower" -> "2000.0",
          "stddev" -> "279.66361883066708", "sum" -> "401690.3973999999")),
        QRRow(Map("count" -> "104", "step" -> "3000 to 3999", "avg" -> "3454.0825326923086", "lower" -> "3000.0",
          "stddev" -> "303.15339002617952", "sum" -> "359224.58340000006"))

      )
    }

    "make field report" in new TestContext {
      new QueryBuilder(meta).withCondition("orderdate", ">=","'2017-01-01'").
        makeFieldStepReport("paid",0,4000,1000).
        data.toList must_=== List(
        QRRow(Map("count" -> "456", "step" -> "0 to 999", "avg" -> "366.61140570175434", "lower" -> "0.0", "stddev" -> "290.62415397914197",
          "sum" -> "167174.801")),
        QRRow(Map("count" -> "145", "step" -> "1000 to 1999", "avg" -> "1434.0531579310341", "lower" -> "1000.0",
          "stddev" -> "285.0479765093578", "sum" -> "207937.70789999995")),
        QRRow(Map("count" -> "43", "step" -> "2000 to 2999", "avg" -> "2447.3348720930239", "lower" -> "2000.0",
          "stddev" -> "304.15692693185895", "sum" -> "105235.39950000003")),
        QRRow(Map("count" -> "17", "step" -> "3000 to 3999", "avg" -> "3386.9346941176468", "lower" -> "3000.0",
          "stddev" -> "268.92665796442651", "sum" -> "57577.8898"))

      )
    }

    "Select by date" in new TestContext {
      new QueryBuilder(meta).withCondition("orderdate", ">", "'2017-01-01'").
        withField("customer").
        doQuery().data.toSet must_=== Set(
        QRRow(Map("customer"-> "693" )))
    }
  }
}
