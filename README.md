### Spark Tutorial with Best Practices
<pre>https://github.com/devmukul44/spark-tutorial</pre>

## Spark Articles
* Anatomy of RDD - slides
<pre>https://www.slideshare.net/datamantra/anatomy-of-rdd</pre>
* Parallel programming in spark - slides
<pre>http://ampcamp.berkeley.edu/wp-content/uploads/2012/06/matei-zaharia-part-1-amp-camp-2012-spark-intro.pdf</pre>
* Spark 1 vs Spark 2
<pre>http://stackoverflow.com/questions/40168779/apache-spark-vs-apache-spark-2</pre>
* Tale of Apache spark's RDD, DataFrame and DataSet
<pre>https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html</pre>
* Spark SQL - Programetically specifying the Schema
<pre>https://www.tutorialspoint.com/spark_sql/programmatically_specifying_schema.htm</pre>

## Best Practices for Unit Test Cases for Spark Framework and Scala Programming Languages

* Done using the scalatest library Funsuite under the org.scalatest.Funsuite
* Interception of errors are done with the help of intercept.
* Store the expected value in mathematical parser in a variable do not directly check it in the assert statement
* Make variable for each value to be passed to the function and standards should be followed.
* Use === to get a good view of the failing of the test.
* The code of mathematical expression parser was changed and map was added as it provides the flexibility to define the key and value for the expression provided.

#### Unit Test Corrections

* wrong 
<pre>
assert(Calculator.check("(-6.3 * 9.4)  - 3") == -62.22)
</pre>

* right

<pre>
val expr = "1/2"
val chart = Map("1" -> 78.0, "2" -> 24.0)
val toBe = chart("1") / chart("2")
</pre>

* create variables for each of the expression and perform the mapping and calculation of result in other variables

* avoid redundant test cases

* wrong

<pre>
assert(Calculator.check("(-6.3 * 9.4)  - 3") == -62.22)
</pre>

* use === to get more clarity on the error message during testing

* right

<pre>
assert(parseObj.sentenceParse(expr) === toBe)
</pre>

* perform all the possible test cases

* wrong 
<pre>
val st = ""
</pre>

* right

<pre>
val expr = ""
</pre>

* use the name of the variables as per their functionality

### Variables list in parsing

* create a list of variables which can be used by the package instead of directly using it in the function

* wrong 
<pre>
def factor: Parser[Double] = (floatingPointNumber ^^ (_.toDouble)
</pre>

* right

<pre>
 private lazy val booleanVariable: Parser[Boolean] = variableMap.keys.toList.sortBy(x => x.length).reverse.map { x =>
    Parser(x)
  }.reduceLeft(_ | _) ^^ (x => variableMap(x))
</pre>

* and then use it in different places


##### Don't write whole statement in single line
* wrong : 
<pre>
val charset = if (optionConfig.hasPath("charset")) {optionConfig.getString("charset")} else { "UTF-8"}
</pre>
* correct : 
<pre>
val charset = if (optionConfig.hasPath("charset")) {
  optionConfig.getString("charset")
} else {
  "UTF-8"
}</pre>
<br /> <br />

##### Correct way to name the tests
* wrong :
<pre>
test("WriteCSVWithoutOptions with saveMode = append")
</pre>
* correct : 
<pre>
test("CSV - Write - Save Mode - Append")
</pre>
<br /> <br />

##### Mention AssertionError for Unit Tests if false
* wrong :
<pre>
assert(mongoObject.getDBName == dbName)
</pre>
* correct : 
<pre>
assert(mongoObject.getDBName == dbName, "DB Name does not match, Mongo Object : " + mongoObject.getDBName + " Actual : " + dbName)
</pre>
<br /> <br />

##### Correct way to compare two dataframes
* wrong :
<pre>
def compareDataFrame(xDF: DataFrame, yDF: DataFrame) {
    assert(xDF.except(selectYDF).count() == 0)
  }
</pre>
* correct : 
<pre>
def compareDataFrame(xDF: DataFrame, yDF: DataFrame) {
    val xColumnNameList = xDF.columns.toList
    val yColumnNameList = yDF.columns.toList
    assert(xColumnNameList.forall(x => yColumnNameList.contains(x)), "Column Name do not match - xDF : " + xColumnNameList.mkString(",") + " - yDF : " + yColumnNameList.mkString(","))
    assert(yColumnNameList.forall(y => xColumnNameList.contains(y)), "Column Name do not match - yDF : " + yColumnNameList.mkString(",") + " - xDF : " + xColumnNameList.mkString(","))
    val selectYDF = yDF.selectExpr(xColumnNameList: _*)
    assert(xDF.except(selectYDF).count() == 0, "DataFrame contents do not match")
  }
</pre>
<br /> <br />

#### creating Sample-dataFrames to Test
* 
<b>
Code should be added in the <i>Utility</i> file for the perticular test and not in <i>Test</i> file.

<pre>
def returnSampleDFType2(sqlContext: SQLContext): DataFrame = {
    sqlContext
      .createDataFrame(
        Seq(
          ("0", "Agriculture"),
          ("1", "Mining"),
          ("2", "Construction"),
          ("3", "Manufacturing"),
          ("4", "Transportation"),
          ("5", "Wholesale Trade"),
          ("6", "Retail Trade"),
          ("7", "Finance")
        ))
      .toDF("code", "industry")
  }
</pre>
<br />
* 
<b> use number of records as per requirements </b><br>
<i> better code for above funtion : </i>

<pre>
def returnSampleDFType2(sqlContext: SQLContext): DataFrame = {
    sqlContext
      .createDataFrame(
        Seq(
          ("0", "Agriculture")
        ))
      .toDF("code", "industry")
  }
</pre>
<br /><br />

#### Variable names should be understandable 
<b> don't use variable name same as the name of objects</b> <br />
<i> It might confuse other developers </i> <br />

##### Example: 
use Variable name as <i> dbName </i> instead of <i> db </i> and <i>collectionName</i> instead of <i>collection</i>
##### other developers can get confused in commands like: 
<pre>
db.createCollection("collectionName")
collection.insertOne(bsonDocument)
</pre>
<b> Better way: </b>
<pre>
dbName.createCollection("collectionName")
collectionName.insertOne(bsonDocument)
</pre>
<br /><br />

#### Correct way to check for Append Option while writing
##### Write twice to check if append works or not and also use basic logic to read the dataframe
* wrong :
<pre>
    val avroWriteFormat = Format(avroWriteConfig)
    avroWriteFormat.writeDataFrame(avroWriteOneDF)
    val outputDF = Extract.getDF(config, sqlContext)
    TestUtility.compareDataFrame(inputDF, outputDF)
    TestUtility.removeResultFiles(filePath)
</pre>
* correct : 
<pre>
val avroWriteFormat = Format(avroWriteConfig)
    avroWriteFormat.writeDataFrame(avroWriteOneDF)
    // Appending to Previously Written Data
    avroWriteFormat.writeDataFrame(avroWriteTwoDF)
    val avroWriteUnionDF = avroWriteOneDF.unionAll(avroWriteTwoDF)
    val avroReadConfigPath = "/com/innovaccer/analytics/core/format/file/AvroRead.json"
    val avroReadConfigResource = getClass.getResource(avroReadConfigPath).getFile
    val avroReadConfigFile = new jfile(avroReadConfigResource)
    val avroReadConfig = ConfigFactory.parseFile(avroReadConfigFile)
    val avroReadFormat = Format(avroReadConfig)
    val outputDF = avroReadFormat.readDataFrame(sqlContext)
    Utility.compareDataFrame(avroWriteUnionDF, outputDF)
    Utility.removeParentFolder(avroWriteFilePath)
</pre>
<br /> <br />

#### Correct way to check for Ignore Option while writing
##### as SaveMode is Ignore - If some file is already present at specified location, it won't write this file to that location 
* wrong :
<pre>
    val avroWriteFormat = Format(avroWriteConfig)
    avroWriteFormat.writeDataFrame(avroWriteOneDF)
</pre>
* correct : 
<pre>
    val avroWriteFormat = Format(avroWriteConfig)
    avroWriteFormat.writeDataFrame(avroWriteOneDF)
    // As Save Mode is Ignore - It won't write this file to specified location
    avroWriteFormat.writeDataFrame(avroWriteTwoDF)

