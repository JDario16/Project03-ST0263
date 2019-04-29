import nltk
import csv
import re
from operator import itemgetter
import unicodedata
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
nltk.download('stopwords')
nltk.download('punkt')

#Lectura de archivos
filelocation = "dbfs:///FileStore/tables/proyecto3/*.csv"

df = spark.read.format("csv").option("header","true").load(filelocation)

#Selecciona las principales columnas para el futuro
content = df.select("id","title","content").collect()

cleaned = []#Lista con la estructura <id:string,title:string,content:list<string>>

#PREPARACION DE DATOS

for row in content:
    text = row.__getitem__("content")
    type(text)
    fila = []
    fila.append(str(row.__getitem__("id")))
    fila.append(str(row.__getitem__("title")))
    cadena = re.sub('[^A-Za-z0-9]+', ' ', str(row.__getitem__("title"))) + ' ' + re.sub('[^A-Za-z0-9]+', ' ', str(row.__getitem__("content")))
    cadena = re.sub(r'\b\w{1,1}\b', '', cadena)

    data = cadena
    stopWords = set(stopwords.words('english'))
    words2 = word_tokenize(data)
    wordsFiltered = []

    for w in words2:
        if w not in stopWords:
            wordsFiltered.append(w)
    fila.append(wordsFiltered)
    cleaned.append(fila)