# Managementul Bazelor de Date - Proiect

Acest proiect demonstreaza utilizarea librariilor SQLAlchemy, oracledb si pandas pentru importul, manipularea si interogarea datelor dintr-un fisier .csv intr-o baza de date Oracle. 
Proiectul include trei cerinte principale, fiecare implementata in Python si documentata in detaliu in acest repository.

## Cerinte si Implementare

1. **Importul datelor si interogari SQL**
   - **Importul datelor**: Datele sunt importate dintr-un fisier .csv utilizand pandas si sunt stocate intr-o tabela Oracle.
   - **Interogari si prelucrari**: Sunt realizate cel putin 10 interogari si prelucrari asupra tabelei, incluzand selectii, filtrari si agregari. Exemple de interogari:
     - Selectarea filmelor dintr-un anumit gen.
     - Calculul mediei scorurilor pentru filmele dintr-un anumit an.
     - Selectarea filmelor regizate de un anumit regizor.

2. **Aplicarea operatorilor si functiilor SQL**
   - **Operatori si functii SQL**: Studierea si aplicarea unor operatori sau functii SQL in SQLAlchemy pe tabela creata. Exemple de operatii:
     - Utilizarea functiei COUNT pentru a numara filmele din fiecare gen.
     - Utilizarea functiei SUM pentru a calcula suma scorurilor.
     - Utilizarea functiei AVG pentru a calcula media scorurilor.

3. **Operatii de actualizare**
   - **Actualizari**: Realizarea a cel putin 3 operatii de actualizare pe tabela creata utilizand SQLAlchemy sau oracledb. Exemple de operatii:
     - Actualizarea scorurilor pentru anumite filme.
     - Modificarea genului unor filme.
     - Stergerea unor filme din tabela.
