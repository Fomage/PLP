Contenu:
Exo2/
Exo3/
Exo5/

Exo2/
  Ce dossier contient le code demandé dans la question 2 sous la forme d'un projet eclipse. Le code proprement dit est dans Exo2/src/cs/bigdata/Tutorial2/
  Le classe main à exécuter est dans le fichier Tree.java. Le fichier arbre.csv doit être dans le PATH (alternativement, son chemin peut être modifié à la ligne 17 du fichier CompterLigneFile.java).

Exo3/ 
  Il s'agit aussi d'un projet eclipse. Le code est dans src. La classe main est dans WordCount.java. Il faut passer en arguments au programme les chemins d'entrée et de sortie dans cet ordre.
  Le mapper associe à chaque mot la paire (mot, 1). Le reducer réduit par clés (mot, 1) pour renvoyer (mot, wordcount(mot)).

Exo5/
  Il s'agit d'un projet eclipse. Le code est dans src. Les questions sont séparées dans différents packages.
  Question 1/
    Les données d'entrées sont spécifiées à la ligne 38 de Tfidf.java. Tf-idf est calculé avec 3 jobs consécutifs.
      Job 1: wordcount
        Mapper: lis mot par mot, renvoie (mot@document, 1)
        Reducer: réduis par clé mot@document, renvoie (mot@document, count(mot@document))
      Job 2: wordcount par document
        Mapper: lis (mot@document, n), renvoie (document,mot=n)
        Reducer: réduis par document, renvoie (mot@document, n/N) où N est le nombre total de mots dans document
      Job 3: calcul des fréquences
        Mapper: lis (mot@document, n/N), renvoie (mot, document=n/N)
        Reducer: réduis par mot, renvoie (mot@document, tfidf(mot, document))
      Job 4: simple tri
        Mapper: lis (mot@document, tfidf), renvoie (-tfidf, mot@document). Ne sert qu'à trier les mots par ordre décroissant de score tfidf.
  Question 2/
    Les donnée d'entrée sont spécifiées à la ligne 38 de PageRank.java. PageRank est calculé avec les jobs suivants:
      Job 1: parse les donnée d'entrée, initialise tous les scores de page rank à 1
    Les jobs 2 et 3 sont répétés un nombre fixe d'itérations. On pourrait remplacer cela par une condition de convergence.
      Job 2: mets à jour la valeur de page rank
        Mapper: lis "A pr B1 B2 B3 ...", qui signifie : la page A a un page rank de pr, et est atteinte via les pages B1, B2,..., Bn; renvoie pour chaque station Bi: (Bi, A pr n)
        Reducer: réduis par Bi, lis des Aj prj nj, renvoie (Bi, pri' A1 A2 ... Aj...)
      Job 3: remet sous le format A pr B1 B2 ...
        Mapper: lis Bi pri A1 A2...Am, renvoie (Aj, Bi) pour tout j dans [1,m] et aussi (Bi, pri)
        Reducer: réduis par Aj, renvoie Aj prj B1 B2 ...
    Enfin, le job 4 trie par ordre de page rank décroissant (cf question 1).
  Question 3/
    Les données d'entrée sont spécifiées dans les jobs principaux. Il suffit d'un seul job pour les 2 premières questions.
    