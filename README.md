# iot_school_mapreduce

## Exercice 1

1. Concevoir un mapper et un reducer pour compter le nombre de mots (regrouper par mot) du fichier texte `20 000 lieues sous les mers-cleaned.txt`
2. Implémenter le mapper et le reducer en Python
3. Implémenter une version multi processeur en Python (https://docs.python.org/3/library/multiprocessing.html)
4. Comment peut-on optimiser le traitement pour qu'il soit en mesure de faire le calcul sur de plus gros volumes de données ?

## Exercice 2

Données en entrées : fichier contenant des données de capteur de température: id_capteur, timestamp, température
1. Proposer un algorithme MapReduce permettant d'obtenir le min et le max par capteur
2. Proposer un algorithme MapReduce permettant d'obtenir le min et le max par mois
3. Proposer un algorithme MapReduce permettant d'obtenir la moyenne par capteur
4. Proposer un algorithme MapReduce permettant d'obtenir la moyenne par capteur en considérant que le volume de données est trop important pour pouvoir faire les calculs sur l'ensemble des clés avec une seule machine