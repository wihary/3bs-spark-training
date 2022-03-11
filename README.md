# 3bs-spark-training
Bienvenue sur le répo contenant un certains nombre de sources permettant de démontrer et exploiter les capacités d'Apache Spark via Scala et C# .Net suivant différents scénarios simplifiés.

# Comment démarrer avec Apache SPARK et .Net
https://docs.microsoft.com/fr-fr/dotnet/spark/tutorials/get-started?tabs=windows

# Comment utiliser les Workbooks

1. Installer Visual Studio Code
1. Ajouter l'extension Name: .NET Interactive Notebooks. https://marketplace.visualstudio.com/items?itemName=ms-dotnettools.dotnet-interactive-vscode
1. Suivre les étapes d'installation de Apache Spark pour Windows
   1. Le Notebook est testé pour la version Spark 3.0.2 et Scala 2.12
1. Démarrer Spark en utilisant la commande suivante (<ins>modifier les chemins d'accès au besoin</ins>) :

``spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local ..\microsoft-spark-3-0_2.12-2.1.0.jar debug``

Assurez vous de récupérer le jar microsoft-spark correspondant a votre version de Spark

5. Ouvrir le notebook
6. Lancer l'execution des steps 