// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val archivo: String = "dbfs:/FileStore/career_change_prediction_dataset.csv"

// COMMAND ----------

val df= spark.read.option("header","true").option("inferSchema","true").csv(archivo)

// COMMAND ----------

display{
  df
}


// COMMAND ----------

val result1 = df.groupBy("Current Occupation")
  .agg(avg("Job Satisfaction").alias("Avg_Job_Satisfaction"))
  .withColumnRenamed("Current Occupation","Current_Occupation")
//¿Cuál es el promedio de satisfacción laboral (Job Satisfaction) según cada ocupación actual (Current Occupation)?


// COMMAND ----------

display (
  result1
)

// COMMAND ----------

val result2 = df.groupBy("Education Level")
.agg(avg("Salary").alias("AVG_Salary"))
.withColumnRenamed("Education Level", "Education Level")
//¿Cómo varía el salario promedio (Salary) entre diferentes niveles de educación (Education Level)?

// COMMAND ----------

display (
  result2
)

// COMMAND ----------

val result3 = df.groupBy("Field of Study", "Gender")
  .count()
  .orderBy("Field of Study", "Gender") 
  //¿Conteo de hombre y mujeres (Gender) por Campo de estudio (Fields of Study)?

// COMMAND ----------

display(
  result3
)

// COMMAND ----------

val result4 = df.groupBy("Field of Study", "Industry Growth Rate")
  .agg(
    count(when(col("Career Change Interest") === "Yes", 1)).as("Interested_Count"), 
    count("*").as("Total_Count") 
  )
  .withColumn("Proportion_Interested", col("Interested_Count") / col("Total_Count")) 
  .orderBy("Field of Study", "Industry Growth Rate")

  //¿Qué proporción de personas está interesada en cambiar de carrera (Career Change Interest) en cada (field of Study) según su tasa de crecimiento (Industry Growth Rate)?

// COMMAND ----------

display(
  result4.withColumnRenamed("Field of Study","Field_of_Study")
  .withColumnRenamed("Industry Growth Rate","Industry_Growth_Rate")
)

// COMMAND ----------

val result5 = df.select("Age", "Education Level")
  .groupBy("Education Level")
  .agg(collect_list("Age").as("Age_List"))
  .orderBy("Age_List") 

  //¿Cuál es la distribución de la edad (Age) por nivel educativo (Education Level)?

// COMMAND ----------

display(
  result5
)

// COMMAND ----------

val result6 = df.groupBy("Gender", "Age", "Education Level", "Job Satisfaction")
  .agg(
    avg("Work-Life Balance").as("Avg_Work_Life_Balance"), 
    count("*").as("Total_Count") 
  )
  .orderBy("Gender", "Age", "Education Level")

  //¿Cómo varía el equilibrio entre trabajo y vida (Work-Life Balance) según el género, la edad, el nivel educativo y la satisfacción laboral?

// COMMAND ----------

display(
  result6.withColumnRenamed("Education Level","Education_Level")
  .withColumnRenamed("Job Satisfaction","Job_Satisfaction")
  
)

// COMMAND ----------

val result7 = df.groupBy("Years of Experience", "Career Change Events")
  .agg(
    count("*").as("Event_Count") 
  )
  .orderBy("Years of Experience", "Career Change Events")
  //¿Qué tan frecuentes son los eventos de cambio de carrera (Career Change Events) por nivel de experiencia laboral (Years of Experience)?

// COMMAND ----------

display(
  result7.withColumnRenamed("Years of Experience","Years_of_Experience")
  .withColumnRenamed("Career Change Events","Career_Change_Eventson")
)

// COMMAND ----------

val result8 = df.groupBy("Current Occupation")
  .agg(
    avg("Work-Life Balance").as("Avg_Work_Life_Balance") 
  )
  .orderBy(desc("Avg_Work_Life_Balance"))

  // ¿Qué ocupaciones actuales (Current Occupation) reportan el mejor equilibrio entre vida y trabajo (Work-Life Balance)?

// COMMAND ----------

display(
  result8.withColumnRenamed("Current Occupation","Current_Occupation")
)

// COMMAND ----------

display(
df
)

// COMMAND ----------


val result9 = df.groupBy("Field of Study")
  .agg(
    count("*").as("total_personas"),
    sum("Geographic Mobility").as("movilidad_geografica")
  )
  .withColumn("proporcion_movilidad", col("movilidad_geografica") / col("total_personas"))

  //¿Qué campos de estudio (Field of Study) tienen la mayor proporción de personas con movilidad geográfica (Geographic Mobility)?

// COMMAND ----------

display(
  result9.withColumnRenamed("Field of Study","Field_of_Study")
)

// COMMAND ----------

val result10 = df.groupBy("Field of Study")
  .agg(
    avg("Job Opportunities").as("promedio_oportunidades"),
    sum("Career Change Events").as("total_cambios"),
    count("*").as("total_personas")
  )
  .withColumn("proporcion_cambios", col("total_cambios") / col("total_personas"))
  .orderBy(asc("promedio_oportunidades"))

  //¿Cuáles son los campos de estudio (Field of Study) con la menor cantidad de oportunidades laborales (Job Opportunities) y cómo afecta esto la proporción de eventos de cambio de carrera (Career Change Events)?

// COMMAND ----------

display(
  result10
)
