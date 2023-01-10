(ns kenai.spark.sql.data-types
  (:import (org.apache.spark.sql.types DataType
                                       DataTypes)))

(def sql-types
  "A mapping from type keywords to Spark types."
  {:str       DataTypes/StringType
   :string    DataTypes/StringType
   :bin       DataTypes/BinaryType
   :binary    DataTypes/BinaryType
   :bool      DataTypes/BooleanType
   :boolean   DataTypes/BooleanType
   :date      DataTypes/DateType
   :timestamp DataTypes/TimestampType
   :interval  DataTypes/CalendarIntervalType
   :double    DataTypes/DoubleType
   :float     DataTypes/FloatType
   :byte      DataTypes/ByteType
   :int       DataTypes/IntegerType
   :integer   DataTypes/IntegerType
   :long      DataTypes/LongType
   :short     DataTypes/ShortType
   :nil       DataTypes/NullType})

(defn- ->data-type
  ^org.apache.spark.sql.types.DataType [x]
  (if (instance? DataType x) x (sql-types x)))

(defn create-array-type
  "Creates an ArrayType by specifying the data type of elements (element-type) and
  whether the array contains null values (contains-null)."
  (^org.apache.spark.sql.types.ArrayType [element-type]
   (create-array-type element-type true))
  (^org.apache.spark.sql.types.ArrayType [element-type contains-null?]
   (DataTypes/createArrayType (->data-type element-type)
                              (boolean contains-null?))))

(defn create-map-type
  "Creates a MapType by specifying the data type of keys (key-type), the data type
  of values (value-type), and whether values contain any null value (value-contains-null)."
  (^org.apache.spark.sql.types.MapType [key-type value-type]
   (create-map-type key-type value-type true))
  (^org.apache.spark.sql.types.MapType [key-type value-type value-contains-null?]
   (DataTypes/createMapType (->data-type key-type)
                            (->data-type value-type)
                            (boolean value-contains-null?))))

(defn create-struct-field
  "Creates a StructField by specifying the name (col-name), data type (dataType)
  and whether values of this field can be null values (nullable)."
  (^org.apache.spark.sql.types.StructField [col-name data-type]
   (create-struct-field col-name data-type true))
  (^org.apache.spark.sql.types.StructField [col-name data-type nullable?]
   (DataTypes/createStructField (name col-name)
                                (->data-type data-type)
                                (boolean nullable?))))

(defn create-struct-type
  "Creates a StructType with the given list of StructFields (fields)."
  ^org.apache.spark.sql.types.StructType [& fields]
  (DataTypes/createStructType ^java.util.List fields))
