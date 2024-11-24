package examen

import org.apache.spark
import org.apache.spark.sql.SparkSession
import examen._
import org.apache.spark.sql.catalyst.dsl.expressions.intToLiteral
import org.apache.spark.sql.functions.{col, desc}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.roaringbitmap.Util.select
import utils.TestInit

import scala.Console.in

class examenTest extends TestInit {


  import spark.implicits._


    //Secuencia de tuplas

    val columnas = Seq("NOMBRE", "EDAD", "CALIFICACION")
    val datos = Seq(
      ("Pilar", 47, 6),
      ("Ruben", 28, 8),
      ("Fernando", 37, 9),
      ("Ariel", 36, 10),
      ("Paola", 40, 7),
      ("Alvaro", 43, 8),
      ("Pedro", 26, 5)
    )


     val estudiantes = datos.toDF(columnas:_*)


   "ejercicio1" should "muestra el esquema del DataFrame" in {
      ejercicio1(estudiantes)(spark)
     }

    "ejercicio1" should "filtrar los estudiantes con una calificacion mayor a 8" in {

        val estudiantesFiltrados = estudiantes.filter(estudiantes("CALIFICACION") > 8)
        estudiantesFiltrados.count() shouldBe 2

        val estudiantesCorrectos = estudiantesFiltrados.filter(estudiantesFiltrados("CALIFICACION") > 8)
        estudiantesCorrectos.count() shouldBe 2
        estudiantesCorrectos.filter(estudiantesCorrectos("NOMBRE") === "Fernando").count() shouldBe 1
        estudiantesCorrectos.filter(estudiantesCorrectos("NOMBRE") === "Ariel").count() shouldBe 1

    }

   "ejercicio1" should "selcecciona los nombres por la calificacion de forma descendente" in {

        val estudiantesOrdenados = ejercicio1(estudiantes)(spark)

    // Verificar que el número de estudiantes filtrados sea correcto (debería haber 2 estudiantes con calificación > 8)
         estudiantesOrdenados.count() shouldEqual 7

    // Verificar que el primer estudiante tenga la mayor calificación (Ariel, 10) y asi sucesivamenete ordenado descendentemente
         estudiantesOrdenados.head().getAs[Int]("CALIFICACION") shouldEqual 10

    // Verificar segundo (Fernando, 9)
         estudiantesOrdenados.head(2).last.getAs[Int]("CALIFICACION") shouldEqual 9

     // Verificar tercero (Ruben, 8)
          estudiantesOrdenados.head(3).last.getAs[Int]("CALIFICACION") shouldEqual 8

    // Verificar cuarto (Alvaro, 8)
           estudiantesOrdenados.head(4).last.getAs[Int]("CALIFICACION") shouldEqual 8

    // Verificar quinto (Paola, 7)
           estudiantesOrdenados.head(5).last.getAs[Int]("CALIFICACION") shouldEqual 7

    // Verificar 6 (Pilar, 6)
           estudiantesOrdenados.head(6).last.getAs[Int]("CALIFICACION") shouldEqual 6

     // Verificar 7 (Pedro , 5)
           estudiantesOrdenados.tail(1).head.getAs[Int]("CALIFICACION") shouldEqual 5

    }

  //Pruebas ejerccio 2

   val numeros = Seq(1,2,3,4,5,6,7,8,9)

   val numerosDF = numeros.toDF("NUMERO")

   "ejercicio 2" should "decir si los numeros son pares o impares"

   val numerosParesImpares = examen.ejercicio2(numerosDF)(spark)
   numerosParesImpares.count() shouldBe 9
   println("DataFrame del ejerccio2")
   numerosParesImpares.show()

   val resultadosexperados = Seq(
   (1, "impar"),
   (2, "par"),
   (3, "impar"),
   (4, "par"),
   (5, "impar"),
   (6, "par"),
   (7, "impar"),
   (8, "par"),
   (9, "impar")
   ).toDF("NUMERO", "resultado")

   println("DataFrame esperado")
   resultadosexperados.show()
   // comprobamos que los dos datframe son iguales

   numerosParesImpares.except(resultadosexperados).count() shouldBe 0



  //Prueba ejerccio3

    "ejercicio3" should "mostrar el esquema de los DataFrames" in {

      import spark.implicits._

      // dataFrame estudiante
      val columnas = Seq("id", "nombre")
      val datos = Seq(
        (1, "Pilar"),
        (2, "Ruben"),
        (3, "Fernando"),
        (4, "Ariel"),
        (5, "Paola"),
        (6, "Alvaro"),
        (7, "Pedro"),
        (8, "Laura"),
        (9, "Marta"),
        (10, "Jorge")
      )
      val estudiantes = datos.toDF(columnas: _*)

      println("Inicio de la prueba")
     /** val caliCsv = spark.read.option("header",true)
        .csv("/Users/jorgegonzalezsaenz/Desktop/KEEPCODING/Big-Data-Processing 2/src/test/resources/examen/notas.csv").
        withColumn("id_estudiante", col("id_estudiantes").cast("Int")).
        withColumn("calificacion", col("calificacion").cast("Double"))
*/
      import org.apache.spark.sql.types._

      val caliCsv = spark.read
        .option("header", true)
        .schema(StructType(
          List(
            StructField("id_estudiantes", IntegerType),
            StructField("asignatura", StringType),
            StructField("calificaciones", DoubleType)
          )
        ))
        .csv("/Users/jorgegonzalezsaenz/Desktop/KEEPCODING/Big-Data-Processing 2/src/test/resources/examen/notas.csv")



      caliCsv.printSchema()
      caliCsv
      //comprobar que el dataframe resultante
      caliCsv.schema.fieldNames should contain allOf("id_estudiantes", "asignatura", "calificaciones")

      //llamo a la funcion
      val resultado = ejercicio3(estudiantes, caliCsv)

      // compruebo si el dataframe tiene las columnas que quiero
      resultado.columns should contain allOf("id_estudiantes", "nombre", "promedio")
      println("Resultado final:")
      resultado





      resultado.select("id_estudiantes", "nombre", "promedio")
        .filter("id_estudiantes = 1")
        .collect() should have size 1





      //mmiro si los valores de la columna pormedio son correctos de cada alumno

      val promedioPilar = resultado.filter(col("id_estudiantes") === 1).select("promedio").collect()(0).getAs[Double]("promedio")
      promedioPilar shouldBe 7.125

      val PromedioEstudiante = Seq(
        (1, "Pilar", 7.125),
        (2, "Ruben", 8.125),
        (3, "Fernando", 7.475),
        (4, "Ariel", 7.25),
        (5, "Paola", 7.275),
        (6, "Alvaro", 7.375),
        (7, "Pedro", 6.85),
        (8, "Laura", 8.95),
        (9, "Marta", 6.525),
        (10, "Jorge", 6.375)

      )
      val promedioEstudianteDF = PromedioEstudiante.toDF("id_estudiantes", "nombre", "promedio")


      resultado.except(promedioEstudianteDF).count() shouldEqual 0
      promedioEstudianteDF.except(resultado).count() shouldEqual 0


    }

  //Prueba ejerccio4
  import spark.implicits._

   "ejerccio4" should "contar la cantidad de ocurrencias de cada palabra" in {
       val palabras = List("mesa", "silla", "mesa", "lampara", "television", "silla",
       "mesa", "silla", "television", "lampara")
       val resultado = ejercicio4(palabras)(spark).collect().toMap

       resultado should contain theSameElementsAs Map(
          "mesa" -> 3,
          "silla" -> 3,
          "lampara" -> 2,
          "television" -> 2
          //"television" -> 3 no pasa la prueba
          // "sofa" -> 0 no pasa la prueba
        )
    }


  //Prueba ejercicio5

  "ejerccio5" should "cargar el archivo" in {
   // Creo DataFrame vacío para pasar como parametro a la funcion
   val ventas = Seq.empty[(String)].toDF()

   // Cargo el archivo para verificar que esta bien el contenido
      val df = spark.read.format("csv").
         option("header", "true").
         option("interSchema", "true").
         option("delimiter", ",").
         load("/Users/jorgegonzalezsaenz/Desktop/KEEPCODING/Big-Data-Processing 2/src/test/resources/examen/ventas.csv")

    // Compruebo que el esquema tiene las columnas esperadas
    df.schema.fieldNames should contain allOf("id_venta", "id_producto", "cantidad", "precio_unitario")

    // Mirar si el numero de filas es el que tiene
    df.count() shouldBe 50
  }


  "ejerccio5" should "comprobar que calcula el ingreso total por producto (cantidad * precio_unitario)" in {

    val ventas = Seq.empty[(String)].toDF()
    val resultado2 = ejercicio5(ventas)(spark)
    resultado2.schema.fieldNames should contain allOf (
      "id_producto", "ingreso_total")
    //Miramos que el dataframe contiene el ingreso total de cada producto
    resultado2.columns should contain ("ingreso_total")

  }

}