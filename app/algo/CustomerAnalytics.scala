package algo

import java.util.UUID

import algo.SelectType.{SelectType, SelectTypeVal}
import com.google.cloud.bigquery.{Job, JobId, JobInfo}

import scala.collection.{Set, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class CustomerAnalytics {

}

object SelectType extends Enumeration {
  type SelectType = SelectTypeVal
  sealed case class SelectTypeVal( isAgg: Boolean) extends Val() {
    def isAggCondition() = isAgg
  }

  val SingleValue = SelectTypeVal(false)
  val sum = SelectTypeVal(true)
  val avg = SelectTypeVal(true)
  val countDistinct = SelectTypeVal(true)
  val date = SelectTypeVal(false)
  val range = SelectTypeVal(false)
  val hierarchy = SelectTypeVal(false)
  val ingroup = SelectTypeVal(false)
}
case class RangeDetails(rangeStart: Int, delta: Int, rangeEnd: String)
case class HierarchyDetails(name: String, level: Int)
case class SelectDetails(selectType: SelectType, range: Option[RangeDetails] = None)

sealed trait FieldData {
  def getTitle: String = ""
  def getName: String = ""
  def getTable: Option[String] = None
  def getSql(defaultTable: String): String = ""
  def getAliasSql(defaultTable: String): String = ""
  def getSelectDetails(): SelectDetails = SelectDetails(SelectType.SingleValue)
  def isAgg = getSelectDetails().selectType.isAggCondition()
}

sealed trait CalcFieldData {
  def getAlias: String = ""
  def getValueFieldName: String = ""
  def getTargetField : String= ""
  def getSql: String = ""
}

case class RegCalcField (builder : QueryBuilder  , summaryField : String , alias : String, targetField : String) extends CalcFieldData {
  override def getAlias: String = alias

  override def getValueFieldName: String = summaryField

  override def getTargetField: String = targetField

  override def getSql: String = {
    s"select customer,$summaryField from  (" +
      builder.getUpdateCustomerSql(summaryField) +
      ") "
  }
}


class WrapperFieldDef(meta : AnalyticsMeta,fld : FieldData, sql : String) extends FieldData {
  override def getTitle: String = fld.getTitle
  override def getName: String = fld.getName
  override def getTable: Option[String] = fld.getTable
  override def getSql(defaultTable: String): String = sql
  override def getAliasSql(defaultTable: String): String = fld.getAliasSql(defaultTable)
  override def getSelectDetails(): SelectDetails = fld.getSelectDetails()
}
class FakeFieldDef(meta : AnalyticsMeta,name: String) extends FieldData {
  override def getName: String = name
}

case class FieldDef(name: String, title: String, table: Option[String], field: String, selectDetails: SelectDetails,
    hierarchy: Option[HierarchyDetails] = None) extends FieldData {
  override def getTitle = title
  override def getName = name

  override def getSql(defaultTable: String) = {
    val f = s" ${table.getOrElse(defaultTable)}.$field  "
    selectDetails.selectType match {
      case SelectType.countDistinct => s"count(distinct $f) "
      case SelectType.sum => s"sum($f) "
      case SelectType.avg => s"avg($f) "
      case SelectType.SingleValue => s"$f "
      case _ => s" $f "
    }
  }
  override def getAliasSql(defaultTable: String) = {
    val f = name
    selectDetails.selectType match {
      case SelectType.countDistinct => s"count(distinct $f) "
      case SelectType.sum => s"sum($f) "
      case SelectType.avg => s"avg($f) "
      case SelectType.SingleValue => s"$f "
      case _ => s" $f "
    }
  }

  override def getTable = table

  override def getSelectDetails(): SelectDetails = selectDetails
}

case class RelationDef(name: String, title: String, field1: String, field2: String) extends FieldData {
  var fld1: FieldDef = _
  var fld2: FieldDef = _
  override def getTitle = title
  override def getName = name
  override def getTable = fld1.table
  def postCreate(analyticsMeta: AnalyticsMeta): Unit = {
    fld1 = analyticsMeta.fieldsMap(field1)
    fld2 = analyticsMeta.fieldsMap(field2)
  }

  override def getSql(defaultTable: String) = s" (${fld1.getSql(defaultTable)} / ${fld2.getSql(defaultTable)})  "
  override def getAliasSql(defaultTable: String) = s" (${fld1.getAliasSql(defaultTable)} / ${fld2.getAliasSql(defaultTable)})  "

  override def getSelectDetails(): SelectDetails = SelectDetails(SelectType.avg)
}

case class ReportMeta(name: String, aggs: List[String])

case class AnalyticsMeta(fields: List[FieldDef], relations: List[RelationDef], reports: List[ReportMeta], customersTable: String, schema: String) {
  var fieldsMap = mutable.Map.empty[String, FieldDef]
  var relationsMap = mutable.Map.empty[String, RelationDef]
  val allFieldsMap = mutable.Map.empty[String, FieldData]
  fieldsMap.++=(fields.map(x => (x.getName, x)))
  relationsMap.++=(relations.map(x => (x.name, x)))
  allFieldsMap.++=(fieldsMap)
  allFieldsMap.++=(relationsMap)
  relations.foreach(x => x.postCreate(this))
  def customerColumn(isSingle : Boolean = false) =
    if (isSingle)
      fieldsMap("customer").copy(selectDetails = SelectDetails(SelectType.SingleValue))
    else
      fieldsMap("customer")
}

case class QRRow(fields: Map[String, String])
case class QueryResult(data: List[QRRow])
case class QCondition(name: String, cond : String, value : String) extends FieldData{

  var fld : FieldData = _
  var schema = ""
  def postCreate(analyticsMeta: AnalyticsMeta): Unit = {
    fld = analyticsMeta.allFieldsMap(name)
    schema= analyticsMeta.schema

  }
  override def getTitle: String = ""

  override def getName: String = ""

  override def getTable: Option[String] = fld.getTable

  override def getSql(defaultTable: String): String =
    (getSelectDetails().selectType, cond) match {
      case (SelectType.ingroup, "=") => s"$defaultTable.customer_id in  (select customer_id from $schema.customer_groups where ${fld.getSql("customer_groups")} = $value) "
      case (SelectType.ingroup, "!=") => s"$defaultTable.customer_id not in  (select customer_id from  $schema.customer_groups where ${fld.getSql("customer_groups")} = $value) "
      case _ => s"${fld.getSql(defaultTable)} ${cond} ${value} "
    }


  override def getAliasSql(defaultTable: String): String = s"${fld.getName} ${cond} ${value} "

  override def getSelectDetails(): SelectDetails = fld.getSelectDetails()
}

object QueryBuilder {
  import com.google.cloud.bigquery.BigQueryOptions

  val bigq = BigQueryOptions.getDefaultInstance.getService

}

import com.google.cloud.bigquery.QueryJobConfiguration

import scala.collection.JavaConversions._

class QueryBuilder(analyticsMeta: AnalyticsMeta) {
  def getUpdateCustomerSql(field : String): String = {
    fieldData.clear()
    val fld = analyticsMeta.fieldsMap(field)
    val cust = analyticsMeta.customerColumn(true)
    val tbl = analyticsMeta.schema + "." + analyticsMeta.customersTable
    fieldData+=cust
    fieldData.+=(fld)
    buildQuerySql()
  }

  def calcCustomerProperties(props : List[CalcFieldData])={
    val sql =
      s"""
        |update ${analyticsMeta.schema}.${analyticsMeta.customersTable}  set
        |${props.map { p =>
        s" ${p.getTargetField} = (select ${p.getValueFieldName} from (${p.getSql}) as ${p.getAlias} " +
          s"where ${analyticsMeta.customersTable}.${analyticsMeta.customerColumn().field} = ${p.getAlias}.customer)"
              }.mkString(",")
           }
         where 1=1
      """.stripMargin
      exec(sql)
  }
  import QueryBuilder._
  import scala.collection.Set
  var fieldData: ArrayBuffer[FieldData] = ArrayBuffer.empty[FieldData]
  def withField(name: String): QueryBuilder = {
    fieldData += analyticsMeta.allFieldsMap(name)
    this
  }

  var conditions = ArrayBuffer.empty[QCondition]
  def withCondition(name: String, op : String, value : String): QueryBuilder = {
    val c = QCondition(name, op, value)
    c.postCreate(analyticsMeta)
    conditions.+=(c)
    this
  }
  def withReport(name : String) : QueryBuilder= {
    analyticsMeta.reports.filter(_.name == name).headOption.foreach{r=>
      r.aggs.foreach(a=>this.withField(a))
    }
    this
  }

  def getMeta = analyticsMeta
  def allTablesButGroup = (fieldData ++ conditions).filterNot(x=>x.getTable.equals(Some("customer_groups")))

  lazy val defaultTable = (allTablesButGroup).flatMap(x => x.getTable).headOption.getOrElse(analyticsMeta.customersTable)
  def getSelectSql = "select " + fieldData.map(x => s"${x.getSql(defaultTable)} as ${x.getName}").mkString(",")
  def getSelectAliasSql(noFurtherAggFields : ArrayBuffer[FieldData]) =
    "select " + fieldData.map(x => s"${if (noFurtherAggFields.contains(x)) x.getName else x.getAliasSql(defaultTable)}  as ${x.getName}").mkString(",")
  def getFromSql = {
    val list = (allTablesButGroup).flatMap(x => x.getTable.map { y => analyticsMeta.schema + "." + y }).toSet

    list.size match {
      case 0 => " from " + analyticsMeta.schema + "." + defaultTable
      case x if (list.size==2 && Set("sales.orders","sales.customers").containsAll(list)) => " from  sales.customers INNER JOIN sales.orders  ON customers.customer_id = orders.customer_id "
      case _ =>  " from " + list.mkString(",")
    }
  }
  def getWhereSql = {
    conditions.filter(x=> ! x.isAgg) match {
      case ArrayBuffer()=> ""
      case conds =>  " where " + conds.map(x => s"${x.getSql(defaultTable)} ").mkString(" and ")
    }
  }
  def getWhereAliasSql = {
    conditions.filter(x=> ! x.isAgg) match {
      case ArrayBuffer()=> ""
      case conds =>  " where " + conds.map(x => s"${x.getAliasSql(defaultTable)} ").mkString(" and ")
    }
  }
  def hasAggInSelectAndWhere(): Boolean = {
    conditions.exists(x => x.isAgg) && fieldData.exists(p=>p.isAgg)
  }

  def getGroupBySql = {
    fieldData.filter(x=> !x.isAgg) match {
      case ArrayBuffer() => ""
      case fields =>  " Group by " + fields.map(x => s"${x.getSql(defaultTable)} ").mkString(",")
    }
  }

  def getGroupByAliasSql = {
    fieldData.filter(x=> Set(SelectType.SingleValue, SelectType.hierarchy).contains(x.getSelectDetails().selectType )) match {
      case ArrayBuffer() => ""
      case fields =>  " Group by " + fields.map(x => s"${x.getName} ").mkString(",")
    }
  }

  def getHavingSql : String = {
    conditions.filter(x=>x.getSelectDetails().selectType.isAggCondition()) match {
      case ArrayBuffer()=> ""
      case conds =>  " having " + conds.map(x => s"${x.getSql(defaultTable)} ").mkString(" and ")
    }
  }

  def getAliasGroupAndConditions(noFurtherAggFields : ArrayBuffer[FieldData]) : String = {
    val where = conditions.filter(x=>noFurtherAggFields.contains(x.fld) )
    val whereSql = if (where.isEmpty) "" else  where.map(x=>x.getAliasSql(defaultTable)).mkString(" where ", " and ", " ")
//    val having = conditions.filter(x=>x.isAgg && !noFurtherAggFields.contains(x.fld) )
//    val havingSql = if (where.isEmpty) "" else  where.map(x=>x.getAliasSql(defaultTable)).mkString(" having ", " and ", " ")
    val group = fieldData.filter(x=>
          Set(SelectType.SingleValue, SelectType.hierarchy).contains(x.getSelectDetails().selectType )
          || noFurtherAggFields.contains(x)
            )

      .map(x => s"${x.getName} ").toSet.mkString(" Group by " ,",","")
    whereSql  + group //+ havingSql
  }

  def duplicateThisBuilder(): QueryBuilder = {
    val res = new QueryBuilder(analyticsMeta)
    fieldData.foreach(x=>res.withField(x.getName))
    conditions.foreach(x=> res.withCondition(x.name,x.cond,x.value))
    res
  }

  def buildQuerySql(): String =
    if (hasAggInSelectAndWhere) {
      val innerBuilder  : QueryBuilder =  duplicateThisBuilder()
      val condAggFields = conditions.filter(x=>x.isAgg).map(y=>y.fld )
      convertAggFieldsToSingleValue( innerBuilder, condAggFields)
      s"${getSelectAliasSql(condAggFields)} from (${innerBuilder.buildQuerySql()})  ${getAliasGroupAndConditions(condAggFields)} "
    }
    else
      getSelectSql + getFromSql + getWhereSql  +  getGroupBySql + getHavingSql

  private def convertAggFieldsToSingleValue( innerBuilder: QueryBuilder, leaveAgg: ArrayBuffer[FieldData]) = {
    innerBuilder.conditions = innerBuilder.conditions.filter(x=>x.isAgg==false)

    innerBuilder.fieldData =  innerBuilder.fieldData.flatMap(x =>
      if (x.isAgg  && !leaveAgg.contains(x) )  analyticsMeta.fieldsMap.get(x.getName).map(y => y.copy(selectDetails = SelectDetails(selectType = SelectType.SingleValue))) else Some(x))
    conditions.map(x=> if (x.isAgg && !innerBuilder.fieldData.exists(_.getName == x.name)) innerBuilder.fieldData.+=( analyticsMeta.fieldsMap(x.name)))
  }

  def makeDateReport(dateField : String, start : Option[String], end : Option[String],  report : String, granularity : String) : QueryResult= {
    withReport(report)
    start.foreach(x=> withCondition(dateField,">=",x))
    end.foreach(x=> withCondition(dateField,"<=",x))
    val fld =  analyticsMeta.fieldsMap(dateField)
    val fldname = fld.getTable.get + "." + fld.field
    val fldsql = granularity match {
      case "daily" => fldname
      case "weekly" => s"FORMAT('%d-%02d',EXTRACT(YEAR FROM $fldname),EXTRACT(WEEK FROM $fldname)) "
      case "monthly" => s"FORMAT('%d-%02d',EXTRACT(YEAR FROM $fldname),EXTRACT(MONTH FROM $fldname)) "
      case "yearly" => s"FORMAT('%d',EXTRACT(YEAR FROM $fldname)) "
    }
    fieldData+=new WrapperFieldDef(analyticsMeta,fld,fldsql)
    doQuery("select * from (" + buildQuerySql() + ") order by " + fld.getName)
  }

  def makeProprtyStepReport(prop : String,  start : Int, end : Int, step : Int) = {
    fieldData.clear()
    val fld = analyticsMeta.fieldsMap(prop)
    val cust = analyticsMeta.customerColumn(true)
    val tbl = analyticsMeta.schema + "." + analyticsMeta.customersTable
    fieldData+=cust
    val inner = buildQuerySql()
    val baseSql = s"select ${cust.field} , ${fld.field} as ${fld.getName} from $tbl where ${cust.field} in ($inner)" +
      s" and ${fld.field} >= $start and ${fld.field} < $end "
    genericMakeStepReport(start,end,step,baseSql,fld.getName)
  }

  def makeFieldStepReport(field : String, start : Int, end : Int, step : Int) = {
    fieldData.clear()
    val fld = analyticsMeta.fieldsMap(field)
    val cust = analyticsMeta.customerColumn(true)
    val tbl = analyticsMeta.schema + "." + analyticsMeta.customersTable
    fieldData+=cust
    fieldData.+=(fld)
    withCondition(field,">=",start.toString)
    withCondition(field,"<",end.toString)
    val inner = buildQuerySql()
    genericMakeStepReport(start,end,step,inner,fld.getName)
  }

  def genericMakeStepReport( start : Int, end : Int, step : Int, baseSql : String, fldname : String) : QueryResult= {
    val fldsql = s"format('%g to %g', $start+(floor(($fldname-$start) / $step) )*$step,($start + $step -1)+(floor(($fldname-$start) / $step) )*$step) "
    val lower = s"$start+(floor(($fldname-$start) / $step) )*$step"
    val sql =
      s"""
        |select $lower as lower, $fldsql as step, sum($fldname) as sum , count(*) as count, avg($fldname) as avg, STDDEV($fldname)  as stddev
        |from ($baseSql)
        |group by lower, step
        |order by lower
      """.stripMargin



    fieldData.clear()
    withFakeFields(List("lower","step","sum","count","avg","stddev"))
    doQuery(sql)
  }

  def makeParetoReport(field : String) = {
    fieldData.clear()
    fieldData+=analyticsMeta.customerColumn(false)
    var total = duplicateThisBuilder().doQuery().data(0).fields("customer").toLong


    fieldData.clear()
    val fld = analyticsMeta.fieldsMap(field)
    val cust = analyticsMeta.customerColumn(true)
    val tbl = analyticsMeta.schema + "." + analyticsMeta.customersTable
    fieldData+=cust
    fieldData.+=(fld)
    val inner = buildQuerySql()
    var res : ArrayBuffer[QRRow] =ArrayBuffer.empty[QRRow]

    var i = 0;
    var max : Option[String] = None
    while (i < 5) {
      val row = genericParetoOneStep(inner,fld.getName, max, if (i < 4)  Some((total * 20) / 100) else None, "Group " + (i+1))
      res += row(0)
      total -= (total * 20) / 100
      max = row(0).fields.get("lower")
      i+=1
    }
    QueryResult(res.toList)
  }

  def genericParetoOneStep( baseSql : String, fldname : String, maxValue : Option[String], maxCusts : Option[Long], group : String) = {
    val whereSql = maxValue.map(x=> s" where $fldname < $x ").getOrElse("")
    val limitSql = maxCusts.map(x=> s" limit $x ").getOrElse("")
    val sql =
      s"""
         |select '$group' as groupname, min($fldname) as lower, max($fldname) as upper, sum($fldname) as sum , count(*) as count, avg($fldname) as avg, STDDEV($fldname)  as stddev
         |from (select * from  ($baseSql)  $whereSql order by $fldname desc $limitSql)
      """.stripMargin



    fieldData.clear()
    withFakeFields(List("groupname","lower","upper","sum","count","avg","stddev"))
    doQuery(sql).data
  }

  def doQuery(): QueryResult = {
    val query = buildQuerySql()
    doQuery(query)
  }
  def doQuery(query : String): QueryResult = {
    println("****" + query)
    val queryConfig = QueryJobConfiguration.newBuilder(query).build
    var data = ArrayBuffer.empty[QRRow]
    Try {
      for (row <- bigq.query(queryConfig).iterateAll) {
        val r = fieldData.map(x => x.getName -> (if (row.get(x.getName).isNull )"" else  row.get(x.getName).getStringValue)).toMap
        data += QRRow(r)
      }
      QueryResult(data.toList)
    }.recover {
      case x: Throwable =>
        println("in throw!!!!")

        x.printStackTrace()
        QueryResult(List())
    }.get
  }


  def saveAsGroup(groupId : String , groupName : String ) = {
    fieldData.clear()
    fieldData.add(analyticsMeta.fields.find(_.name=="customer").get.copy(selectDetails = SelectDetails(selectType = SelectType.SingleValue)))
    val sql = buildQuerySql()
    println("****" + sql)
    exec( s"delete from sales.customer_groups where group_id = '$groupId'")
    exec( s"insert into sales.customer_groups(customer_id,group_id) select customer,  '$groupId' as group_id from ($sql)")

  }


  def exec(sql : String): Int = {
    println("****" + sql)
    val queryConfig = QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build //legacy : ture , standard : false


    // Create a job ID so that we can safely retry.
    var queryJob : Job= null
    var jobId : JobId = null
    // Wait for the query to complete.
    Utils.tryForTwentySeconds (
      {
        jobId = JobId.of(UUID.randomUUID.toString)
        queryJob = bigq.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build)
        queryJob = queryJob.waitFor()

      },20
    )


    // Check for errors
    if (queryJob == null) throw new RuntimeException("Job no longer exists")
    else if (queryJob.getStatus.getError != null) { // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus.getError.toString)
    }
    val res= bigq.getQueryResults(jobId)
    0
  }
  def withFakeFields(fields : List[String]) = {
    fields.foreach(x=>fieldData+=new FakeFieldDef(analyticsMeta,x))
    this
  }


}


object Utils  {
  def tryForTwentySeconds(code: => Unit, times: Int): Unit = {
    Try{
        code
    }.recoverWith { case e =>
      if (times == 0)
        Failure(e)
      else {
        println("*******************TRYING AGAIN***********************")
        Thread.sleep(500)
        Success(tryForTwentySeconds(code, times - 1))
      }
    }
  }
  def ignoreCode(code: => Unit): Unit = {

  }

}

//object BigQueryTest extends App {
//
//  import com.google.cloud.bigquery.FieldValue
//  import com.google.cloud.bigquery.FieldValueList
//  import com.google.cloud.bigquery.QueryJobConfiguration
//  import com.google.cloud.bigquery.BigQuery
//  import com.google.cloud.bigquery.BigQueryOptions
//
//  val bigquery = BigQueryOptions.getDefaultInstance.getService
//  val query = "SELECT * FROM `sales.orders` ;"
//  val queryConfig = QueryJobConfiguration.newBuilder(query).build
//
//  import bigquery4s._
//  val bq = bigquery4s.BigQuery()
//
//  // Print the results.
//  import scala.collection.JavaConversions._
//
//  val cur = System.currentTimeMillis()
//  val schema = bigquery.query(queryConfig).getSchema
//  (0 to schema.getFields.size()).foreach { i =>
//    val fld = schema.getFields.get(i)
//  }
//  for (row <- bigquery.query(queryConfig).iterateAll) {
//    import scala.collection.JavaConversions._
//    for (`val` <- row) {
//      System.out.printf("%s,", `val`.toString)
//    }
//    System.out.printf("\n")
//  }
//  println(s"${System.currentTimeMillis() - cur}")
//}
