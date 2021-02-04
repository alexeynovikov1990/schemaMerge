package tempdsread

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import java.util.IdentityHashMap
import java.nio.file.Files
import java.nio.file.Paths

class DataReader {

  def addSchema(objId:String) : Unit = {
    val schemaPath = objId + "_schema.ddl"
    val schemaDDL = Files.readAllBytes(Paths.get(schemaPath)).map(_.toChar).mkString
    val schema = StructType.fromDDL(schemaDDL)
    val fldSet = new mutable.HashSet[StructField]() ++= schema.fields
    objectsId += objId
    fieldSets.put(objId, fldSet)
  }

  def readAll() : DataFrame = {
    if (processSchema()) {
      val frames = new ArrayBuffer[DataFrame]()
      for (objid <- objectsId) {
        val df = Spark.spark.read.parquet(objid)
        frames += normalizeDataframe(objid, df)
      }
      var result = frames(0)
      for (i <- 1 until frames.size)
        result = result.union(frames(i))
      result
    }
    else{
      //Если схемы не отличаются
      //То все фреймы зачитываются в 1 без каких либо
      //изменений
      Spark.spark.read.parquet(objectsId.toList: _*)
    }
  }

  private def processSchema() : Boolean = {
    toRename.clear()
    toAppend.clear()
    mapFieldsToObjects()
    val sum = getSetSum()
    val isec = getSetIntersection()
    val difference = sum.diff(isec)
    if (!difference.isEmpty){
      collectInfo(difference)
      true
    }
    else false
  }

  private def normalizeDataframe(objId:String, df:DataFrame) : DataFrame = {
    var newDf = df
    for (f <- toRename.get(objId).get){
      val columnName = f
      val newColumnName = columnName + "_" + objId.hashCode
      newDf = newDf.withColumnRenamed(columnName, newColumnName)
    }
    for (f <- toAppend.get(objId).get){
      newDf = newDf.withColumn(f.name, lit("").cast(f.dataType))
    }
    val sortedNames = newDf.columns.sortWith(_.compareTo(_) < 0)
    newDf.select(sortedNames.head, sortedNames.tail: _*)
  }

  private def mapFieldsToObjects() : Unit = {
    val iter = fieldSets.iterator
    while (iter.hasNext){
      val p = iter.next()
      val objid = p._1
      for (f <- p._2){
        fieldsToObjects.put(f, objid)
      }
    }
  }

  private def getSetSum() : HashSet[StructField] = {
    var sum = new mutable.HashSet[StructField]()
    for (s <- fieldSets){
      sum = sum ++= s._2
    }
    sum
  }

  private def getSetIntersection() : HashSet[StructField] = {
    var isec = new HashSet[StructField]()
    val iter = fieldSets.iterator
    if (iter.hasNext) {
      isec = fieldSets.iterator.next()._2
      while (iter.hasNext) {
        isec = isec intersect iter.next()._2
      }
    }
    isec
  }

  private def checkFieldByName(fields:mutable.HashSet[StructField], name:String) : Boolean = {
    var result = false
    val iter = fields.iterator
    while (iter.hasNext){
      val f = iter.next()
      if (f.name.equals(name)){
        result = true
      }
    }
    result
  }

  private def resolveOneConflict(dif:mutable.HashSet[StructField], fieldName:String) : ArrayBuffer[StructField] = {
    val result = new ArrayBuffer[StructField]()
    val typesCount = new HashSet[StructField]()
    val iter = dif.iterator
    while (iter.hasNext){
      val fld = iter.next()
      if (fld.name.equals(fieldName)){
        val conflictObjid = fieldsToObjects.get(fld)
        result += new StructField(fld.name + "_" + conflictObjid.hashCode.toString, fld.dataType, true)
        typesCount += fld
      }
    }
    if (typesCount.size > 1) result
    else new ArrayBuffer[StructField]() += typesCount.head
  }

  private def resolveConflits(dif:mutable.HashSet[StructField], toAppend:HashSet[String]) : ArrayBuffer[StructField] = {
    val result = new ArrayBuffer[StructField]()
    for (name <- toAppend){
      result ++= resolveOneConflict(dif, name)
    }
    result
  }

  private def collectInfo(dif:mutable.HashSet[StructField]) : Unit = {

    for (objid <- objectsId) {
      val rename = new HashSet[String]()
      val renamedAppend = new ArrayBuffer[StructField]()
      var append = new HashSet[String]()
      val objFields = fieldSets.get(objid).get
      val iter = dif.iterator
      while (iter.hasNext) {
        val f = iter.next()
        if (!objFields.contains(f)){
          //Если поля с таким именем и типом не нашлось
          //то возможно 2 варианта
          if (checkFieldByName(objFields, f.name)){
            //Если существует поле с таким именем, но другим типом, то такая ситуация называется конфликтом
            //В этом случае поле должно быть переименовано
            rename += f.name
            //К тому же в набор renamedAppend должно быть добавлено
            //соответствующее поле объекта, вступившего в конфликт
            val conflictObjid = fieldsToObjects.get(f)
            renamedAppend += new StructField(f.name + "_" + conflictObjid.hashCode.toString, f.dataType, true)
          }
          else{
            //Если же не найдено соответствия даже по имени,
            //то такой столбец надо добавить
            append += f.name
          }
        }
      }
      //Объект может добавить поле, по которому вступают в конфликт
      //иные объекты. Потому необходима проверка новых колонок на участие в
      //конфликтах и переименование при необходимости
      val resolved = resolveConflits(dif, append)
      toRename.put(objid, rename)
      toAppend.put(objid, resolved ++= renamedAppend)
    }
  }

  def getFieldsToRename(objid:String) : Option[HashSet[String]] = toRename.get(objid)
  def getFieldsToAppend(objid:String) : Option[ArrayBuffer[StructField]] = toAppend.get(objid)

  private val fieldSets = new HashMap[String, mutable.HashSet[StructField]]()
  private val objectsId = new mutable.HashSet[String]()
  private val toRename = new mutable.HashMap[String, mutable.HashSet[String]]()
  private val toAppend = new mutable.HashMap[String, ArrayBuffer[StructField]]()
  private val fieldsToObjects = new IdentityHashMap[StructField, String]()
}
