import nltk
import csv
import re
from operator import itemgetter
import unicodedata
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
nltk.download('stopwords')
nltk.download('punkt')

def limpieza(cleaned):
  filelocation = "dbfs:///FileStore/tables/proyecto3/*.csv"

  df = spark.read.format("csv").option("header","true").load(filelocation)

  df.select("id","title","content")

  content = df.select("id","title","content").collect()

  cleaned = []

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
  return cleaned
    

def indice_invertido(words):
  search_word = "" #Aca se almacena la palabra a buscar
  # Obtiene valor del widget dbutils.widgets.get(name='widget_01')
  inverted = []
  for y in range(len(words)):
    freq = words[y][2].count(search_word)
    if freq > 0:
      inverted.append([freq, words[y][0], words[y][1]])

  inverted = sorted(inverted, key=lambda x: x[0])
  inverted.reverse()
  for i in inverted[:5]:
    print(i)

#dbutils.widgets.text(name='widget_01', defaultValue='', label = 'Ingrese la palabra ') # Abre widget que permite escribir la palabra a buscar
words = []
words = limpieza(words)

indice_invertido(words)