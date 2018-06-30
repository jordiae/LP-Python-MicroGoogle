import pickle

import urllib.request
from bs4 import BeautifulSoup
import urllib.parse

from util import clean_word
from util import clean_words

import ast


#############################################################################
# Common part
#############################################################################


def authors():
    """Returns a string with the name of the authors of the work."""

    # Please modify this function

    return "Jordi Armengol Estapé"


#############################################################################
# Crawler
#############################################################################


def store(db, filename):
    with open(filename, "wb") as f:
        print("store", filename)
        pickle.dump(db, f)
        print("done")

# Nota: els comentaris seran en catala normatiu pero sense accents, per evitar
# possibles problemes d'encoding.

# Originalment havia fet que el diccionari fos url -> contingut de la web,
# pero llavors answer era mes lent i db ocupava mes en disc.

# He primat molt l'eficiencia d'answer a canvi de fer més ineficient el
# crawling.
# Durant el crawling, cada paraula apareguda en alguna de les webs tindra
# una entrada en el diccionari amb que representem la base de dades.
# El valor associat a cada clau sera la llista de webs que
# la contenen (es a dir, els parells (titol,url)). Aixo permetra que les
# consultes des de la web siguin molt mes rapides. En el cas base,
# simplement retornarem el valor de l'entrada de la paraula buscada.
# Per les queries complexes, farem operacions de conjunts (unions i
# interseccions). Aquesta gran rapidesa es perd si fem les proves des
# d'answer.py directament, perque aquest script cada cop que es fa
# una nova consulta ha de carregar tot l'arxiu, i al ser enorme tarda
# bastant. Esta pensat per maximitzar la velocitat suposant que la
# base de dades nomes es carrega al principi, o es va actualitzant
# nomes de tant en tant. En aquesta solucio, la cerca " "
# es a dir, espai en blanc, no produira resultats. Podriem fer
# una entrada al diccionari per l'espai amb blanc amb totes les webs,
# pero no tindria massa sentit.


def crawler(url, maxdist):
    """
        Crawls the web starting from url,
        following up to maxdist links
        and returns the built database.
    """

    # Please implement this function

    dict = {}  # db: diccionari (paraula apareguda -> [(titol,url) de web)]

    webs = []  # llista de webs a tractar
    webs.append(tuple((url, 0)))  # la url original, amb profunditat 0
    # A webs guardarem totes les webs a guardar, amb parells (url,depth)
    # Realment, no podem saber si un link es HTML o no fins que no l'obrim
    # i mirem el content type. AMb l'extensio no n'hi ha prou.
    # Per exemple, apple.com/education es HTML perque el servidor serveix
    # el corresponent index.html

    # for (web, depth) in webs:
    i = 0
    used_webs = set([url])  # webs ja visitades
    # utilitzem while en comptes d'un iterador perque la llista es
    # dinamica (es fan appends dins de l'iteracio), i llavors queda
    # mes clar aixi.
    while i < len(webs):
        (web, depth) = webs[i]
        try:
            # timeout de 3, el valor per defecte es massa gran
            response = urllib.request.urlopen(web, timeout=3)
            content_type = response.info().get('Content-Type')
            # Posem aquesta condicio en comptes de
            # content_type = 'text/html', perque algunes webs
            # indiquen mes coses al content type
            if "html" not in content_type:
                break
            page = response.read()
            soup = BeautifulSoup(page, "html.parser")

        # Si salta alguna excepcio sera la corresponent a un error
        # a l'establir la connexio (de sockets, handshake...) o
        # HTTP (404 not found, 403 forbidden...)
        except Exception as e:
            print(e)
            i += 1
            continue
        try:
            # Com hem dit, cada paraula trobada en alguna de les
            # webs tindra una entrada al diccionari amb el
            # conjunt de les webs (titol,url) on aparegui
            title = clean_words(soup.title.text)
            text = clean_words(soup.get_text())
            content = title + text
            added_words = set([])
            # Per totes les paraules del contingut de la web
            for word in content.split():
                if word in added_words:
                    continue
                if word not in dict:
                    dict[word] = set([tuple((title, web))])
                else:
                    dict[word].add(tuple((title, web)))
                added_words.add(word)

        # Es podria produir una excepcio si una web no tingues
        # contingut, o si per algun caracter especial falles
        # clean_words
        except Exception as e:
            print(e)
        # Afegim tots els links no repetits i amb protocol hhtp
        # o https
        # Si ja estem a la maxima profunditat, no mirarem cap link
        # Sumarem 1 a la profunditat dels fills, respecte la del pare
        if depth < maxdist:
            for link in soup.find_all("a"):
                try:
                    newurl = urllib.parse.urljoin(web, link.get("href"))
                    (newweb, fragment) = urllib.parse.urldefrag(newurl)
                    if newweb.startswith("http") and newweb not in used_webs:
                        used_webs.add(newweb)
                        webs.append(tuple((newweb, depth + 1)))
                # urljoin pot provocar una excepcio si la url
                # esta mal formada
                except Exception as e:
                    print(e)
                    continue
        i += 1
    return dict


#############################################################################
# Answer
#############################################################################


def load(filename):
    """Reads an object from file filename and returns it."""
    with open(filename, "rb") as f:
        print("load", filename)
        db = pickle.load(f)
        print("done")
        return db

# A answer simplement aprofitarem el diccionari. En el cas
# base, retornarem el valor de l'entrada de la paraula.
# En els altres, per [] farem interseccions i per ()
# farem unions. Amb [], si arribem a un conjunt buit
# ja sabem que per mes interseccions continuara sent
# el conjunt buit. Amb (), no tenim aquesta propietat
# (hauriem de fer-ho amb el conjunt de totes les
# paraules)

# Seria trivial incloure una cache amb memoritzacio pels
# casos no trivials.
# Tindriem un parametre cache={}, i:
# if query in cache: return cache[query]
# Abans de fer el return del calcul si no estigues a
# la cache, hauriem de guardar-hi el valor.
# NO l'he inclos perque crec que no podiem modificar la
# capcalera de la funcio, encara que seria un parametre
# opcional.
# La cache no pot estar a answer_aux perque alla
# ja tractem amb llistes i tuples, que no son hashejables


def answer(db, query):  # , cache={})
    # Funcio auxiliar recursiva que tracta directament els
    # elements com a objectes de Python
    def answer_aux(q):
        ans = set([])
        if isinstance(q, list):  # and
            ans = answer_aux(q[0])
            for elem in q:
                ans = ans.intersection(answer_aux(elem))
                if len(ans) == 0:
                    break  # "lazy evaluation"
        elif isinstance(q, tuple):  # or
            ans = answer_aux(q[0])
            for elem in q:
                ans = ans.union(answer_aux(elem))
        # cas base
        else:
            if q in db:
                ans = set(db[q])
            else:
                ans = set([])

            # Segons la documentacio query ja esta cleaned
            # "given a database and a query (that is, a string
            # of cleaned words)", pero penso que no.
            # Si no ho hem d'assumir, hauriem de fer clean
            # a la paraula del cas base. Aixo afectaria
            # l'eficiencia d'answer.
            """
            try:
                word = clean_word(q)
                if word in db:
                    ans = set(db[word])
                else:
                    ans = set([])
            except:
                ans = set([])
            """
        return ans

    # not_base = (query[0] == '[' or query[0] == '(')
    # if query in cache and not_base:
    #    return cache[query]

    qu = query
    if query[0] == '[' or query[0] == '(':
        qu = ast.literal_eval(query)

    # result = list(answer_aux(qu))
    # if not_base: cache[query] = result
    # return result
    return list(answer_aux(qu))
