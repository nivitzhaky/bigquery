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
        .withCondition("orderid", ">", "2")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("productcategory" -> "Office Supplies", "customer"-> "673" )),
        QRRow(Map("productcategory" -> "Furniture", "customer"-> "297")),
        QRRow(Map("productcategory" -> "Technology", "customer"-> "237"))
      )
    }

    "Select category customer with sum purchases" in new TestContext {
      new QueryBuilder(meta).withField("customer").withField("paid")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("paid" -> "2297200.8602999565", "customer"-> "793" ))
      )
    }

    "Select category customer with sum purchases and product" in new TestContext {
      new QueryBuilder(meta).withField("customer").withField("paid")
        .withCondition("productcategory", "=", "'Office Supplies'")
        .doQuery().data.toSet must_=== Set(
        QRRow(Map("paid" -> "719047.03200000094", "customer"-> "788" ))
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

    "make property steps report" in new TestContext {
      new QueryBuilder(meta).withCondition("orderdate", ">=","'2017-01-01'").
        withCondition("orderid", ">=","3").
        makeFieldStepReport("sumpurchases",0,4000,1000).
        data.toList must_=== List(
           QRRow(Map("count" -> "25", "step" -> "0 to 999", "avg" -> "649.79527200000007", "lower" -> "0.0", "stddev" -> "252.20585008064265",
             "sum" -> "16244.881800000001")),
            QRRow(Map("count" -> "58", "step" -> "1000 to 1999", "avg" -> "1485.5780637931036", "lower" -> "1000.0", "stddev" -> "295.93945259912135",
              "sum" -> "86163.527700000021")),
          QRRow(Map("count" -> "71", "step" -> "2000 to 2999", "avg" -> "2557.4598211267607", "lower" -> "2000.0", "stddev" -> "252.40335758803036",
            "sum" -> "181579.6473")),
          QRRow(Map("count" -> "53", "step" -> "3000 to 3999", "avg" -> "3482.2186301886795", "lower" -> "3000.0", "stddev" -> "283.82107053578306",
            "sum" -> "184557.58740000011"))

      )
    }

    "Select by date" in new TestContext {
      new QueryBuilder(meta).withCondition("orderdate", ">", "'2017-01-01'").
        withField("customer").
        doQuery().data.toSet must_=== Set(
        QRRow(Map("customer"-> "693" )))
    }

    "Pareto by property" in new TestContext {
      new QueryBuilder(meta).makeParetoReport("sumpurchases").
        data.toSet must_=== Set(
        QRRow(Map("groupname" -> "Group 1","count" -> "158", "avg" -> "6973.2999120253144", "lower" -> "4299.1609999999991", "upper" -> "25043.05",  "stddev" -> "2991.5897053518584", "sum" -> "1101781.3861000002")),
        QRRow(Map("groupname" -> "Group 2","count" -> "127", "avg" -> "3530.774589763781", "lower" -> "2955.2259999999997", "upper" -> "4282.9400000000005",  "stddev" -> "393.88534441997012", "sum" -> "448408.3728999999")),
        QRRow(Map("groupname" -> "Group 3","count" -> "101", "avg" -> "2634.7045435643563", "lower" -> "2332.577", "upper" -> "2945.321", "stddev" -> "186.2841436764613", "sum" -> "266105.15890000004")),
        QRRow(Map("groupname" -> "Group 4","count" -> "81", "avg" -> "2087.249885185186", "lower" -> "1824.234", "upper" -> "2305.712",  "stddev" -> "150.52371487391252", "sum" -> "169067.24069999994")),
        QRRow(Map("groupname" -> "Group 5","count" -> "326", "avg" -> "956.56043466257609", "lower" -> "4.833", "upper" -> "1821.7420000000002",  "stddev" -> "492.40885078403176", "sum" -> "311838.70170000009"))
      )
    }
    "Pareto by sales" in new TestContext {
      new QueryBuilder(meta).withCondition("orderdate", ">", "'2017-01-01'").makeParetoReport("paid").
        data.toSet must_=== Set(
        QRRow(Map("groupname" -> "Group 1","count" -> "138", "avg" -> "3181.3417499999978", "lower" -> "1603.478", "upper" -> "14203.277999999998",  "stddev" -> "2083.5070374467546", "sum" -> "439025.16150000005")),
        QRRow(Map("groupname" -> "Group 2","count" -> "111", "avg" -> "1235.1399792792786", "lower" -> "920.61799999999994", "upper" -> "1590.116",  "stddev" -> "190.85963430283599", "sum" -> "137100.5377")),
        QRRow(Map("groupname" -> "Group 3","count" -> "88", "avg" -> "784.26676136363665", "lower" -> "637.18000000000006", "upper" -> "919.22000000000014", "stddev" -> "83.1418713315116", "sum" -> "69015.474999999977")),
        QRRow(Map("groupname" -> "Group 4","count" -> "71", "avg" -> "541.062152112676", "lower" -> "454.368", "upper" -> "637.024",  "stddev" -> "52.422165088415596", "sum" -> "38415.412800000006")),
        QRRow(Map("groupname" -> "Group 5", "count" -> "285", "avg" -> "169.04154456140347", "lower" -> "1.188", "upper" -> "449.312", "stddev" -> "129.43672213788426", "sum" -> "48176.840200000064"))
      )
    }

  }
}
