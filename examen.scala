package examen

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._


import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object examen {


  /** Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   * Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   * e studiantes (nombre, edad, calificación).
   * R ealiza las siguientes operaciones:
   *
   * M uestra el esquema del DataFrame.
   * F iltra los estudiantes con una calificación mayor a 8.
   * S elecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   * */

    def ejercicio1(estudiantes: DataFrame)(spark: SparkSession): DataFrame = {
       // Mostrar el esquema del DataFrame
       estudiantes.show()
       //estudiantes

       val estudiantesFiltrados =estudiantes.filter(estudiantes("CALIFICACION") > 8)
       estudiantesFiltrados.show()
       //estudiantesFiltrados

       val estudiantesFilDescCali = estudiantes.orderBy(desc("CALIFICACION"))
       estudiantesFilDescCali.show()
       estudiantesFilDescCali

    }


  //////////////////////////////

    /**Ejercicio 2: UDF (User Defined Function)
   * Pregunta: Define una función que determine si un número es par o impar.
   * Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   * */

   def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {

    // funcion que determina si el número es par o impar

      def parOimpar(numero:Int): String = {
         if (numero % 2 == 0)
            "par"
         else {
            "impar"
         }
      }


   import spark.implicits._

   val udfparOimpar = udf((numero:Int) =>
   if (numero % 2 == 0)
      "par"
   else {
      "impar"
   }
    )

   val numerosResultados = numeros.withColumn("resultado", udfparOimpar(col("NUMERO")))

    numerosResultados.show()
    numerosResultados

    }





  /** Ejercicio 3: Joins y agregaciones *Pregunta: Dado dos DataFrames,
   * uno  con información de estudiantes (id, nombre)
   * y o tro con calificaciones (id_estudiante, asignatura, calificacion),
   * rea liza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
 def ejercicio3(estudiantes: DataFrame, calificaciones: DataFrame): DataFrame = {

    println("Datos estudiantes")
    estudiantes.show()
    estudiantes

    println("Datos calificaciones")
    calificaciones.show()
    calificaciones


    //Cambiamos el "id" del DataFrame de estudiantes por "id_estudiantes" para que coincidan


    val estudiantesNuevos = estudiantes.withColumnRenamed("id", "id_estudiantes")
    val resultadoJoin = estudiantesNuevos.join(calificaciones, "id_estudiantes")
    val resultadoFinal = resultadoJoin.select("id_estudiantes", "nombre", "calificaciones")

    println("Resultado del join")
    resultadoFinal.show()
    resultadoFinal

    //Calcular el promedio de las calificaciones por cada estudiante

    val PromedioCalificacionesEstudaiantes = resultadoFinal.
      groupBy("id_estudiantes","nombre").
      agg(avg("calificaciones").alias("promedio")).
      orderBy("id_estudiantes")

    println("Promedio de calificaciones por estudiantes:")
    PromedioCalificacionesEstudaiantes.show()
    PromedioCalificacionesEstudaiantes


  }




   /**Ejercicio 4: Uso de RDDs
   *Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
 *
 **/

 def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {

   //Creamos un RDD a partir de una lista de palabras

   val RDDpalabras = spark.sparkContext.parallelize(palabras)

   // Paso cada palabra en un par (palabra, 1)
   val paresPalabra = RDDpalabras.map(palabra => (palabra, 1))

   //contamos la cantidad de ocurrencias de cada palabra
   val ocurrenciasPalabra = paresPalabra.reduceByKey(_ + _)
   ocurrenciasPalabra

 }

  /**
   *Ejercicio 5: Procesamiento de archivos
   *Pregunta: Carga un archivo CSV que contenga información sobre
   *ventas (id_venta, id_producto, cantidad, precio_unitario)
   *y calcula el ingreso total (cantidad * precio_unitario) por producto.
   **/

  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {
    //cargo el archivo CSV
    val df = spark.read.format("csv").
      option("header", "true").
      option("interSchema", "true").
      option("delimiter", ",").
      load("/Users/jorgegonzalezsaenz/Desktop/KEEPCODING/Big-Data-Processing 2/src/test/resources/examen/ventas.csv")

    df.printSchema()
    df.show()

    df
    // Añado una columna nueva al DataFrame "ingreso" que es el cantidad * precio_unitario por cada producto
    val IngresoDf = df.withColumn("ingreso", col("cantidad") * col("precio_unitario"))
    IngresoDf.printSchema()
    IngresoDf.show()
    IngresoDf

    // Agrupo por id_proucto y sumos sus ingresos totales

    val resultado = IngresoDf.groupBy("id_producto").agg(sum("ingreso").alias("ingreso_total"))
    resultado.printSchema()
    resultado.show()
    resultado



  }



}