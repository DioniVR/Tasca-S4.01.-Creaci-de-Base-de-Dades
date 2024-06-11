/*Nivell 1 
Descàrrega els arxius CSV, estudia'ls i dissenya una base de dades amb un esquema d'estrella que contingui
, almenys 4 taules de les quals puguis realitzar les següents consultes: */

# 1 - Tenemos que crear una base de datos que llamaremos Transacciones 

CREATE DATABASE transacciones;

#2- Creamos la primera tabla maestro llamada companies e importamos los datos; 

CREATE TABLE companies ( 

company_id    VARCHAR(15) PRIMARY KEY, 

company_name VARCHAR(255), 

phone  VARCHAR(15), 

email   VARCHAR(100), 

country   VARCHAR(100), 

Website  VARCHAR(100) ); 

select * from companies;


/*Procedemos a cargar el fichero csv con los datos con la siguiente instrucción LOAD DATA. Me da el siguiente error
 "error code: 1290. the mysql server is running with the --secure-file-priv option so it cannot execute this statement".
 En internet, he encontrado que eso significa que sólo podemos cargar los datos que están en una determinada carpeta por
 seguridad.
 Con la siguiente instrucción, obtenemos la carpeta en la que debemos guardar el archivo.
 SHOW VARIABLES LIKE "secure_file_priv"
 Resultado de la anterior busqueda : C:\ProgramData\MySQL\MySQL Server 8.0\Uploads\
 Sólo podemos cargar ficheros CSV que estén en ese directorio.*/
 
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/companies.csv" INTO TABLE companies
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

select * from companies;


#3 –Creamos la tabla maestro credit_cards e importamos los datos: 

CREATE TABLE credit_cards ( 

id VARCHAR(15) PRIMARY KEY, 

user_id INT, 

iban VARCHAR(50), 

pan VARCHAR(30), 

pin VARCHAR(4), 

cvv INT, 

track1 VARCHAR(200), 

track2 VARCHAR(200), 

expiring_date VARCHAR(10)
); 

# Cargamos los datos de credit cards:

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/credit_cards.csv" INTO TABLE credit_cards
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

SELECT * FROM credit_cards;


#4 _ –Creamos la tabla maestro users e importamos los datos: 


 

CREATE TABLE users ( 

Id INT PRIMARY KEY, 

name VARCHAR(100), 

surname VARCHAR(100), 

phone  VARCHAR(100), 

Email VARCHAR(150), 

birth_date VARCHAR(100), 

country VARCHAR(150), 

city VARCHAR(150), 

postal_code VARCHAR(100), 

address VARCHAR(125)); 




# Cargamos los datos de users_ca:

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users_ca.csv" INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;


# Cargamos los datos de users_uk:

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users_uk.csv" INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

# Cargamos los datos de users_usa:

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users_usa.csv" INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS;

/*Las tablas usuarios nos ha dado problemas de carga porque hay saltos de páginas y   algunos campos  están incluido entre comillas.
 Por eso añadimos  ENCLOSED BY Y LINES TERMINATED BY. */
 
 SELECT * FROM users;


#5 – Creamos la tabla de hechos transactions e insertamos los datos.

 

CREATE TABLE transactions  ( 

Id VARCHAR(255), 

card_id VARCHAR(15), 

business_id VARCHAR(25), 

Timestamp TIMESTAMP, 

Amount DECIMAL (10,2), 

Declined TINYINT(1), 

product_ids  VARCHAR(20),

user_id  INT,

Lat FLOAT, 

Longitude FLOAT, 

CONSTRAINT FK_user_id  FOREIGN KEY (user_id) REFERENCES users(id), 

CONSTRAINT FK_card_id FOREIGN KEY (card_id) REFERENCES credit_cards(id), 

CONSTRAINT FK_business_id FOREIGN KEY (business_id) REFERENCES companies(company_id)
); 


# Hemos olvidado decir que el campo ID de la tabla transactions será la primary key. Haremos un ALTER TABLE


ALTER TABLE transactions
ADD PRIMARY KEY (id);


# Cargamos los datos en la tabla transactions.

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions.csv" INTO TABLE transactions
FIELDS TERMINATED BY ';'
IGNORE 1 ROWS;


SELECT * FROM transactions;

/*Exercici 1 
Realitza una subconsulta que mostri tots els usuaris amb més de 30 transaccions utilitzant almenys 2 taules*/ 

/*Subconsulta: unimos las tablas transactions y users con un JOIN y luego agrupamos las transacciones por usuarios (contamos el número 
transacciones) y ordernamos por campo suma transacciones orden descendiente*/
 
SELECT users.Id, users.name, users.surname, count(transactions.id) AS transacciones
FROM transacciones.transactions
JOIN transacciones.users
ON transactions.user_id = users.Id
GROUP BY users.Id, users.name, users.surname
HAVING transacciones > 30
ORDER BY 3 DESC;


#Query final. Con la consulta anterior haremos un derived table de la cual extraeremos los datos que necesitamos:

SELECT subquery.ID, subquery.Name, subquery.Surname, subquery.transacciones
from (	SELECT users.Id as ID, users.name AS Name, users.surname AS Surname, count(transactions.id) AS transacciones
		FROM transacciones.transactions
		JOIN transacciones.users
		ON transactions.user_id = users.Id
		GROUP BY users.Id, users.name, users.surname
		HAVING transacciones > 30
		ORDER BY 3 DESC) subquery;
	
/*-Exercici 2 
Mostra la mitjana d'amount per IBAN de les targetes de crèdit a la companyia Donec Ltd, utilitza almenys 2 taules. */

/*Tenemos que unir las tablas transactions, credit_cards y companies, para obterner los campos IBAN, Amount y Company_name
Después agrupamos por iban, hacermos el promedio del amount y añadimos una condición en el WHERE para indicar que la compañia
ha de ser la Donec LTD.*/

SELECT credit_cards.iban, ROUND(AVG(transactions.Amount),2)
FROM transacciones.transactions
JOIN transacciones.credit_cards
ON transactions.card_id = credit_cards.id
JOIN transacciones.companies
ON transactions.business_id= companies.company_id 
WHERE companies.company_name = "Donec Ltd"
GROUP BY credit_cards.iban;



/*Nivell 2
Crea una nova taula que reflecteixi l'estat de les targetes de crèdit basat en si les últimes tres transaccions van ser declinades 
i genera la següent consulta:*/

#Tenemos que empezar creando la tabla que nos piden

/*Subquery : Con esta subquery sacaremos las 3 últimas transacciones que tiene cada tarjeta. Usaremos el comando ROWN_NUMBER()
con el cual haremos una partición de la columna card_id y seleccionaremos las tres últimas transacciones de cada tarjeta.*/

SELECT card_id, declined, timestamp, ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS rownum
FROM   transactions;

/*Usamos la anterior subquery para hacer una "derived table", de la cual cogeremos la tarjeta de crédito,  declined y timestamp.
Agrupamos por card_id y sumamos por declined
Finalmente haremos usando el comando CASE crearemos una nueva columna en la que incluiremos el comentario si está activa o no*/

SELECT card_id AS id, SUM(declined) AS declined,
CASE
    WHEN SUM(declined) >=3  THEN "INACTIVA"
    ELSE "ACTIVA"
END AS comments
FROM (SELECT card_id AS card_id, declined AS declined, timestamp, ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS rownum
FROM   transactions) as subquery
WHERE subquery.rownum <=3
GROUP BY card_id ;

#Resultado de la query: Todas las tarjetas están activas.

#Creamos una tabla en la  para la tarjeta: card_status

CREATE TABLE Card_Status( 
id VARCHAR(15) PRIMARY KEY,
declined TINYINT(1),
comments VARCHAR(25));


#Insertamos los datos de la la busqueda en nueva tabla

INSERT INTO card_status(id,declined,comments)
SELECT card_id AS id, SUM(declined) AS declined,
CASE
    WHEN SUM(declined) >=3  THEN "INACTIVA"
    ELSE "ACTIVA"
END AS comments
FROM (SELECT card_id AS card_id, declined AS declined, timestamp, ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY timestamp DESC) AS rownum
FROM   transactions) as subquery
WHERE subquery.rownum <=3
GROUP BY card_id;

#Consulta para ver que todos los datos se han cargado bien



SELECT * FROM CARD_STATUS;


# Ahora tenemos que enlazar esta nueva tabla card_status con la tabla credit_cards Lo haremos con el ALTER TABLE

ALTER TABLE card_status
ADD CONSTRAINT FK_id FOREIGN KEY (id) REFERENCES credit_cards(id);

/*Exercici 1 Quantes targetes estan actives?**/

SELECT COUNT(id)
FROM card_status
WHERE comments = "ACTIVA";

#Salen 275 activas que son todas las tarjetas.


/*Nivell 3 
Crea una taula amb la qual puguem unir les dades del nou arxiu products.csv amb la base de dades creada,
 tenint en compte que des de transaction tens product_ids. Genera la següent consulta*/
 
# 1 - Creamos la tabla product e insertamos los datos. 


CREATE TABLE products ( 

Id INT PRIMARY KEY, 

product_name VARCHAR(255), 

price DECIMAL (10,2), 

colour VARCHAR(100), 

weight DECIMAL (10,2), 

warehouse_id  VARCHAR(25) 

); 


# Cargamos los datos en la tabla products.

LOAD  DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/products.csv" INTO TABLE products
FIELDS TERMINATED BY ','
IGNORE 1 ROWS;

/*La carga de este fichero nos dió problemas por que el campo amount está en unidades de $*/


select * from products;
select * from transactions;


/*Tenemos un problema. En la columna product_id de la tabla Transactions hay varios valores en un mismo campo. Tenemos que separarlos.
 Primero, haremos una tabla donde separaremos los valores separados por comas en columnas mediantes la funcion subtring_index().
 Renombraremos cada columna un product_id  diferente (P1, P2, P3 y p4). */


SELECT id,product_ids,
substring_index(product_ids,',',1) AS P1,
substring_index(substring_index(product_ids,',',2), ',', -1) AS P2, 
/*Con el primer string cogeremos los dos primeros valos, y con el segundo Substring nos quedaremos con el segundo valor*/
substring_index(substring_index(product_ids,',',3), ',', -1) AS P3,
# Al igual que arriba, sacamos tres valores, y nos quedaremos con el tercero
substring_index(product_ids,',',-1) AS P4
# Nos quedamos con el último valor.
FROM transactions;

/* Con la formula anterior, obtendremos algunas columnas con product_ids repetidos. Pero eso no es problema.
Más adelante, cuando unamos las tablas más adelantes, usaremos un "UNION" y no  un "UNION ALL". Eso hará que si hay filas repetidas,
las elimnine.

Ahora haremos utilizaremos la tabla anterior como si fuera una derived table y haremos cuatros selects que uniremos con  UNION

SELECT ID, P1 FROM DERIVED TABLE
UNION
SELECT ID, P2 FROM DERIVED TABLE
UNION
SELECT ID, P3 FROM DERIVED TABLE
UNION
SELECT ID, P4 FROM DERIVED TABLE


Pero esto tiene un problema es que es una consulta estática. Si el día de mañana, hubiera un ticket con más de cuatro productos
esta consulta se quedaría insuficiente. Tenemos que automatizar esta consulta.

Para automatizar la función SUBSTRING_INDEX (string, delimiter, number) tendremos que hacer que el argumento number de la
función Substring_index sea un elemento cambiante, es decir, que vaya cambiando según el número de productos que contiene cada registro*/

SELECT 
    transactions.id,
    SUBSTRING_INDEX(SUBSTRING_INDEX(TRANSACTIONS.PROducT_IDS, ',', numero.num ), ',', -1) AS Product_id
FROM transactions;

    
/*Para ello, definiremos una  "Derived table"  llamada numero con una columna que se llame Num.*/

/*Esta nueva tabla números será la  unión de  6 SELETCS ya que está pensada para que tenga un máximo de seis productos. Si se quiere incluir más 
de seis productos en la transacción habrá que ampliarlo.

 Para crear la "Derived table" llamada número  :
 
 Select 1 as  Num Union all  Select 2 Union all Select 3 Union all Select 4 Union all Select 5 Union all Select 6 Union all

Haremos un Join de esta tabla con la consulta anterior.

La consulta final quedará así: */

SELECT 
    transactions.id,
    SUBSTRING_INDEX(SUBSTRING_INDEX(TRANSACTIONS.PROducT_IDS, ',', numero.num ), ',', -1) AS Product_id

FROM transactions

JOIN 

( Select 1 as  Num Union all  Select 2 Union all Select 3 Union all Select 4 Union all Select 5 Union all Select 6) AS numero

ON

 LENGTH(TRANSACTIONS.PROducT_IDS) -LENGTH(REPLACE(TRANSACTIONS.PROducT_IDS, ',', '') ) + 1 >= Numero.num;

/* con la funcion anterior, los Num de la tabla Numero irán sustituyendo el numero.num de  la función Substring_index cuando se vaya haciendo el JOIN.

Ahora bien, hay transacciones que tienen dos productos, otros tres. ¿Cómo instruimos para sepan cuantos productos tiene cada transacción ?

Lo haremos mediante la condición  ON.

1 - LENGTH(TRANSACTIONS.PROducT_IDS) --- Indica cuántos dígitos tiene el cada campo incluyendo las comas y los espacios.

2 - LENGTH(REPLACE(TRANSACTIONS.PROducT_IDS, ',', '') --- Primero le decimos que elimine las comas. Y luego pedimos que sume los dígitos.

3 - Si a la resta de los dos anterior le sumamos 1, nos da cuantos product_ids tenemos. Le sumamos +1 porque el último productos no de cada transacción
no va seguida de una coma. De no sumar uno, nos dejaríamos un producto en cada transaccion.

LENGTH(TRANSACTIONS.PROducT_IDS) -LENGTH(REPLACE(TRANSACTIONS.PROducT_IDS, ',', '')

4 - En el ON indicamos que el numero de productos que hay en cada transacción, ha de ser mayor al número de la tabla numero.Num

LENGTH(TRANSACTIONS.PROducT_IDS) -LENGTH(REPLACE(TRANSACTIONS.PROducT_IDS, ',', '') ) + 1 >= Numero.num   */

#Resultado de la query;

# #Creamos una tabla id_productid

CREATE TABLE Transaction_product (
Transaction_id VARCHAR(255),
product_id  INT NOT NULL,
CONSTRAINT Transaction_product PRIMARY KEY(Transaction_id,product_id)
);



# #Añadimos los datos a la Transaction_product

INSERT INTO Transaction_product (Transaction_id, product_id)

SELECT 
    transactions.id as Transaction_id,
    SUBSTRING_INDEX(SUBSTRING_INDEX(TRANSACTIONS.PROducT_IDS, ',', numero.num ), ',', -1) AS Product_id

FROM transactions

JOIN 

( Select 1 as  Num Union all  Select 2 Union all Select 3 Union all Select 4 Union all Select 5 Union all Select 6) AS numero

ON LENGTH(TRANSACTIONS.PROducT_IDS) -LENGTH(REPLACE(TRANSACTIONS.PROducT_IDS, ',', '') ) + 1 >= Numero.num;

#Consulta para ver los resultado de la tabal Transaction_product

SELECT * FROM Transaction_product
ORDER BY 1;



#Ahora tenemos que enlazar las tablas Transaction_product con las tablas transactions y products

# Enlace Transaction_product con la tabla transactions

ALTER TABLE Transaction_product
ADD CONSTRAINT FK_transactionID FOREIGN KEY (Transaction_ID) REFERENCES transactions(id);

# Enlace Transaction_product con la tabla products


ALTER TABLE Transaction_product
ADD CONSTRAINT FK_productid FOREIGN KEY (product_id) REFERENCES products(id);

/*Exercici 1
Necessitem conèixer el nombre de vegades que s'ha venut cada producte.*/

/*Para esta consulta usaremos la tabla recientemente creada TransactionID_productID. Agruparemos por producto y contaremos el número de
transacciones*/

select product_id,product_name, count(transaction_id)
from Transaction_product
JOIN products
ON PRODUCTS.ID = Transaction_product.product_id
group by product_id
order by 1 ;

...



    


















































































































