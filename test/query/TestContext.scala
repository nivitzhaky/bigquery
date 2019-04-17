package query

import algo.{FieldDef, _}
import org.specs2.matcher.MustThrownExpectations
import org.specs2.specification.Scope
trait TestContext extends Scope with MustThrownExpectations {
  val meta = AnalyticsMeta(
    fields = List(
      FieldDef("orderid", "Sales", Some("orders"), "Order_Id", SelectDetails(SelectType.countDistinct)),
      FieldDef("orderdate", "Order Date", Some("orders"), "Order_Date", SelectDetails(SelectType.date)),
      FieldDef("customer", "Customers", None, "Customer_ID", SelectDetails(SelectType.countDistinct)),
      FieldDef("sumpurchases", "Sum. Purchases", Some("customers"), "sum_purchases", SelectDetails(SelectType.sum)),
      FieldDef("numpurchases", "#. Purchases", Some("orders"), "Order_Id", SelectDetails(SelectType.countDistinct)),
      FieldDef("segment", "Segment", Some("orders"), "Segment", SelectDetails(SelectType.SingleValue)),
      FieldDef("region", "Region", Some("orders"), "Region", SelectDetails(SelectType.hierarchy), Some(HierarchyDetails("location", 0))),
      FieldDef("country", "Country", Some("orders"), "Country", SelectDetails(SelectType.hierarchy), Some(HierarchyDetails("location", 1))),
      FieldDef("state", "State", Some("orders"), "State", SelectDetails(SelectType.hierarchy), Some(HierarchyDetails("location", 2))),
      FieldDef("productcategory", "P.Category", Some("orders"), "Category", SelectDetails(SelectType.hierarchy), Some(HierarchyDetails("product", 1))),
      FieldDef("productsubcategory", "P.Sub.Category", Some("orders"), "Sub_Category", SelectDetails(SelectType.hierarchy), Some(HierarchyDetails("product", 2))),
      FieldDef("product", "Product", Some("orders"), "Product_Name", SelectDetails(SelectType.hierarchy), Some(HierarchyDetails("product", 2))),
      FieldDef("productcount", "Product#", Some("orders"), "Product_Name", SelectDetails(SelectType.countDistinct)),
      FieldDef("paid", "Paid", Some("orders"), "Sales", SelectDetails(SelectType.sum)),
      FieldDef("Quantity", "Amount", Some("orders"), "Quantity", SelectDetails(SelectType.sum)),
      FieldDef("avgdiscount", "Avg. Discount", Some("orders"), "Discount", SelectDetails(SelectType.avg)),
      FieldDef("group_id", "Group", Some("customer_groups"), "group_id", SelectDetails(SelectType.ingroup)),
      FieldDef("profit", "Profit", Some("orders"), "Profit", SelectDetails(SelectType.sum))
    ),
    List(
      RelationDef("avgpurchase", "Avg.purchase", "paid", "orderid")
    ),
    List(
      ReportMeta("default", List("customer", "orderid", "productcount", "avgdiscount", "avgpurchase"))
    ),
    customersTable = "customers", schema = "sales"

  )

}
