// Databricks notebook source
// MAGIC %md
// MAGIC # Import libraries
// MAGIC

// COMMAND ----------

// Importing libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import spark.sqlContext.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._

// COMMAND ----------

// MAGIC %md
// MAGIC # Partie1 : Load Data

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 1
// MAGIC
// MAGIC - Lien Github vers les données:
// MAGIC
// MAGIC   - [Data_TP2_MSD.txt](https://github.com/Atji00/Atji_projects/blob/Main/Databricks_spark/Data_TP2_MSD.txt)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Création de la fonction: getEtudiantsAndProf():

// COMMAND ----------

// Creation de fonctions pour importer les données

def getEtudiantsAndProf(path: String): Seq[DataFrame] = {

    // Importation des données complètes
        
        val data = spark.read.option("multiline", true).json(path)
        
    // Extraction des données Etudiants

        val Etudiants = data.withColumn("Etudiants", explode($"Etudiants"))
                            .select($"Etudiants.*")
                            .withColumn("Universite", split($"CodeFormation","@")(0))
                            .withColumn("Niveau", split($"CodeFormation","@")(1))
                            .withColumn("Formation", split($"CodeFormation","@")(2))

    // Extraction des données Professeurs
  
        val Profs = data.withColumn("Profs", explode($"Profs"))
                        .select($"Profs.*")
                      //.withColumn("Codeformation", explode($"CodeFormations"))
                      //.drop("CodeFormations")

        return Seq(Etudiants, Profs)
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Application de la fonction aux données

// COMMAND ----------

// chemin d'accès aux données
val path = "/FileStore/tables/Data_TP2_MSD-1.txt"

// COMMAND ----------

// Lecture des données etudiants

val dfEtudiants = getEtudiantsAndProf(path)(0)

dfEtudiants.show
 

// COMMAND ----------

// Lecture des données Profs

val dfProfs = getEtudiantsAndProf(path)(1)

dfProfs.show

// COMMAND ----------

// MAGIC %md
// MAGIC # Partie 2 Agrégation simple

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question1 :
// MAGIC  fonction calculant la somme d’argent versée par Niveau d’étude pour chaque université

// COMMAND ----------

// Definition de la fonction

def SumBourseByNivAndUniv(df:DataFrame): DataFrame ={

    val data = df.groupBy("Universite","Niveau")
                 .agg(sum("Bourse_Exel") as "Sum_Bourse_Exel")
    return data
}

// Application de la fonction

SumBourseByNivAndUniv(dfEtudiants).orderBy("Universite", "Niveau").show

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question2 :
// MAGIC fonction calculant le nombre de bourse donné dans une université pour chaque année

// COMMAND ----------

// Definition de la fonction

def CountBourseUnivEachYear(df:DataFrame): DataFrame ={

    val data = df.groupBy("Universite")
                 .pivot("Annee")
                 //.agg(count("*"))
                 .count()
                 .na.fill(0)
    
    return data
}

// Application de la fonction

CountBourseUnivEachYear(dfEtudiants).show

// COMMAND ----------

// MAGIC %md
// MAGIC # Partie3: agrégation avec windows partition
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question1 :
// MAGIC Donne les informations de l’etudiant qui perçoit la somme d’argent la plus importante pour chaque
// MAGIC université au cours d’une année

// COMMAND ----------

// Definition de la fonction

def TopOnBourseForUnivEachYear(df:DataFrame): DataFrame = {

    val window_spec = Window.partitionBy($"Universite", $"Annee").orderBy(desc("Bourse_Exel"))

    val data = df.withColumn("Rank", rank().over(window_spec))
                 .filter(col("Rank")===1)
                 .drop("Rank")
    return data
}

// Application de la fonction

TopOnBourseForUnivEachYear(dfEtudiants).orderBy("Annee", "Universite")
                                       .show

// COMMAND ----------

// MAGIC %md
// MAGIC ### Question2 :
// MAGIC Calcule l’écart de bourse pour chaque année entre le plus petit et le suivant (plus proche)

// COMMAND ----------

// Definition de la fonction

def DiffBetwMinAndNex(df:DataFrame): DataFrame = {

    val window_spec = Window.partitionBy($"Annee").orderBy(asc("Bourse_Exel"))
    val value = "c'est le min"
    val data = df.withColumn("lag", lag("Bourse_Exel",1).over(window_spec))
                 .withColumn("dif_between_next", $"Bourse_Exel" - $"lag")
                 .withColumn("dif_between_next", coalesce($"dif_between_next", lit(value))) 
    return data
}

// Application de la fonction

DiffBetwMinAndNex(dfEtudiants).show

// COMMAND ----------

// MAGIC %md
// MAGIC # Partie 4: Agrégation Combinée
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 1 :
// MAGIC Faire une agrégation combinée de l’année et université en utilisant la fonction cube

// COMMAND ----------

def CubeUniversiteAndAnnee (df:DataFrame): DataFrame = {

      val data = df.cube("Universite", "Annee")
                   .agg(sum("Bourse_Exel") as "Sum_Bourse_Exel")
                   //.na.fill(Map("Universite"->"ALL_Univ", "Annee"->"ALL_Year"))
                   .withColumn("Universite",coalesce($"Universite", lit("ALL_Univ")))
                   .withColumn("Annee", coalesce($"Annee", lit("ALL_Year")))
      return data
}

// Application de la fonction

CubeUniversiteAndAnnee(dfEtudiants)
                                  //.orderBy(desc("Universite"), $"Annee")
                                  .show

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 2 :
// MAGIC - a) Réaliser le graphe de la question Question1 en utilisant l’Api SQL

// COMMAND ----------

val cube_table = CubeUniversiteAndAnnee(dfEtudiants)
                                              //.orderBy(desc("Universite"), $"Annee")
                                              //.withColumn("Universite",concat($"Universite", lit(", "), $"Annee"))
cube_table.show                                  

// COMMAND ----------

// Créer une vue temporaire pour le DataFrame
cube_table.createOrReplaceTempView("cube_table")
// Requête SQL pour sélectionner les données nécessaires pour le graphe
val grapheData = spark.sql("""
  SELECT Annee, Universite, Sum_Bourse_Exel
  FROM cube_table
  --ORDER BY Universite DESC
""")

grapheData.show( )
//display(grapheData)

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # Importer Matplotlib
// MAGIC import matplotlib.pyplot as plt
// MAGIC from matplotlib.ticker import FuncFormatter
// MAGIC
// MAGIC # Convertir les données Spark en Pandas
// MAGIC graphe_df = spark.sql("SELECT * FROM cube_table").toPandas()
// MAGIC
// MAGIC
// MAGIC # Fonction pour formater les valeurs en "k"
// MAGIC def format_k(x, pos):
// MAGIC     if x == 0: # x est la valeur numerique a formater
// MAGIC         return '0'
// MAGIC     return f'{int(x/1000)}k'
// MAGIC # Préparer les données, pour combiner les colonnes années et universite
// MAGIC categories = graphe_df['Annee'] + ', ' + graphe_df['Universite']
// MAGIC values = graphe_df['Sum_Bourse_Exel']
// MAGIC # Créer le graphique
// MAGIC plt.figure(figsize=(10, 6))
// MAGIC plt.bar(categories, values, color="#1f77b4")
// MAGIC # Ajouter des titres et des étiquettes
// MAGIC plt.xlabel("Annee, Universite", fontsize=12)
// MAGIC plt.ylabel("Sum_Bourse_Exel", fontsize=12)
// MAGIC # Appliquer le formateur à l'axe Y
// MAGIC formatter = FuncFormatter(format_k)
// MAGIC # Appliquer le formateur à l'axe Y
// MAGIC plt.gca().yaxis.set_major_formatter(formatter)
// MAGIC # Ajuster les limites de l'axe X pour enlever les espaces
// MAGIC plt.gca().margins(x=0)  # Supprimer les marges horizontales
// MAGIC # Supprimer complètement les ticks sur X et Y
// MAGIC plt.gca().tick_params(axis='x', length=0)  
// MAGIC plt.gca().tick_params(axis='y', length=0)
// MAGIC # Enlever les bordures en haut et à droite
// MAGIC plt.gca().spines["top"].set_visible(False)
// MAGIC plt.gca().spines["right"].set_visible(False)
// MAGIC plt.gca().spines["left"].set_visible(False)
// MAGIC # Ajouter une grille interne
// MAGIC plt.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.7)
// MAGIC # Ajuster la rotation pour lisibilité
// MAGIC plt.xticks(rotation=70)
// MAGIC plt.tight_layout()
// MAGIC # Afficher le graphique
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC - b) Tirez une conclusion
// MAGIC
// MAGIC La visualisation de ce graphe et l'analyse des données nous montrent des tendances intéressantes avec des pics notables, particulièrement, pour certaines années et universités:
// MAGIC
// MAGIC - Pour l'université UCAD, on observe une augmentation de 480% entre les années 2017 et 2019.
// MAGIC
// MAGIC - Pour l'université de Thiès, on observe une augmentation de 220% entre les années 2019 et 2020.
// MAGIC
// MAGIC - Pour l'université UGB, les données sont disponible que sur une année.
// MAGIC
// MAGIC
// MAGIC On observe une progression significative dans le soutien financier apporté aux étudiants pour les universités de Thiès et UCAD.
// MAGIC
// MAGIC Concernant les valeurs totales "All_Univ" des différentes années:
// MAGIC
// MAGIC - 2017, ce total concerne uniquement l'université UCAD
// MAGIC - 2019, ce total concerne l'UCAD, UGB et Thiès
// MAGIC - 2020, ce total concerne uniquement l'université de Thiès

// COMMAND ----------

// MAGIC %md
// MAGIC # Partie 5: Cross data

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 1 :
// MAGIC La fonction fait la jointure du Dataframe etudiant et celui de prof afin de savoir quel est l’id du prof
// MAGIC récompensé pour chaque étudiant.
// MAGIC Si l’étudiant n’a pas de prof récompensé la valeur de l’idProf sera ProfS_No_Recompensés

// COMMAND ----------

//Partie 5 Question 1

def CheckProfRecomp(dfEtu:DataFrame, dfPr:DataFrame):DataFrame = {
 
      val data = dfEtu.join(dfPr, array_contains(dfPr("CodeFormations"),
                            dfEtu("CodeFormation")) && dfEtu("Annee") === dfPr("Annee"),
                            "left" 
                            )
                      .withColumn("IdProf", coalesce(col("IdProf"), lit("Profs_No_Recompensés")))
                      .drop("CodeFormation","CodeFormations").drop(dfPr.col("Annee"))
      return data
}

// Application de la fonction

CheckProfRecomp(dfEtudiants, dfProfs).show
