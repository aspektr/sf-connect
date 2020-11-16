# Data transfer from SalesForce to S3 

##Example

```scala
import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import com.amazonaws.services.glue.DynamicFrame 
import scala.collection.JavaConverters.mapAsJavaMapConverter
import com.azur.drive.sfconnect._

//1. Define parameters
val doInitialLoad = "n" //if you're going to load data first time specify "y"

val dwhURL = "jdbc:redshift://***.redshift.amazonaws.com:5439/db"
val dwhUser = "user"
val dwhPsw = "password"
val dwhTable = "sf.user"
val auditColNameInDWH = "last_modified_date"

val auditColNameInSFObject = "LastModifiedDate"

val username = "user"
val password = "password"
val securityToken = "****"
val passwordWithSecurityToken = password + securityToken
val clientId = "****"
val clientSecret = "***"
val url = "https://login.salesforce.com/"
val version = "49.0"

val sfObject = "User"
val bulk = "false"
val inferSchema = "true"
val pkChunking = "true"
val chunkSize = "100000"
val queryAll = "true"

val outPath = s"s3://sf-stg/${sfObject.toLowerCase}/${sfObject}IncrementalLoad/"

//2. Define predicate for incremental load
lazy val last_modified_date = sparkSession.read.
    format("jdbc").
    option("url", dwhURL).
    option("query", s"select max($auditColNameInDWH) as max_modified_date  from $dwhTable").
    option("user", dwhUser).
    option("password", dwhPsw).
    load().
    head.
    getTimestamp(0).
    toString.
    replace(" ", "T").
    replace(".0", "z")

val predicate = if (doInitialLoad != "y") s" where $auditColNameInSFObject > $last_modified_date" else ""

//3.Get incremental data from SF
val sfInstance = SF(username=username,
    password = password,
    clientId = clientId,
    clientSecret = clientSecret)

def prodData(soql:String) = sparkSession.read.format("com.springml.spark.salesforce").
    option("soql",soql).
    option("username", username).
    option("password", passwordWithSecurityToken).
    option("login", url).
    option("version", version).
    option("sfObject", sfObject).
    option("bulk", bulk).
    option("pkChunking", pkChunking).
    option("chunkSize", chunkSize).
    option("queryAll", queryAll)

val df = sfInstance.soql(sfObject, predicate, "https://company-name.my.salesforce.com/") match {
    case Right(soql) => prodData(soql).load()
    case Left(_) => sparkSession.createDataFrame(List())
}

//4. Change data types
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DecimalType

val dict = Map(
    "datetime"->TimestampType,
    "date"->DateType,
    "double"->DoubleType,
    "int"->IntegerType,
    "boolean"->BooleanType,
    "currency"->DoubleType,
    "percent"->DoubleType
)

def changeColType(cols: List[String],
                  objMetadata: Map[String, String],
                  res: org.apache.spark.sql.DataFrame):org.apache.spark.sql.DataFrame = {
    if(cols.isEmpty) res
    else changeColType(cols.tail,
                      objMetadata,
                      res.withColumn(
                          cols.head,
                          col(cols.head).cast(
                              dict.getOrElse(objMetadata(cols.head),StringType)
                          )
                      )
                      )       
    }

val dfChangedTypes = sfInstance.describe(sfObject, sfUrl) match {
    case Right(objMetadata) => changeColType(df.columns.toList.filter(!_.contains(".")), objMetadata, df)
    case Left(_) => sparkSession.emptyDataFrame
}

//Add load_at column and save to S3

if (dfChangedTypes.count != 0) {
    //add load_at column
    import org.apache.spark.sql.functions.current_timestamp 
    import java.util.Calendar
    
    val year: Int = Calendar.getInstance().get(Calendar.YEAR)
    val month :Int = Calendar.getInstance().get(Calendar.MONTH) + 1
    val day: Int = Calendar.getInstance().get(Calendar.DATE)
    val hour: Int = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + 3
    val minute: Int = Calendar.getInstance().get(Calendar.MINUTE)
    
    val dfWLoadTime = dfChangedTypes.withColumn("s3_load_at", current_timestamp()).
        withColumn("year_load_at", lit(year)).
        withColumn("month_load_at", lit(month)).
        withColumn("date_load_at", lit(day)).
        withColumn("hour_load_at", lit(hour)).
        withColumn("minute_load_at", lit(minute))
        
    //save to S3
    val dfToSave = DynamicFrame(dfWLoadTime, glueContext).withName(s"$sfObject").withTransformationContext(s"${sfObject}ToDynamicFrame")
    val sink = glueContext.getSinkWithFormat(connectionType = "s3", 
                                             options = JsonOptions(Map("path" -> outPath,  "partitionKeys" -> List("year_load_at", "month_load_at", "date_load_at", "hour_load_at", "minute_load_at"))),
                                             format = "parquet",
                                             transformationContext = s"${sfObject}ToS3")
    val res = sink.writeDynamicFrame(dfToSave)
}
```
    
    