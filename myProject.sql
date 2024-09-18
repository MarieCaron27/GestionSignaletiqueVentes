--Creation utilisateur (dans sys) :

CREATE USER ProjetSGBD IDENTIFIED BY ProjetSGBD
DEFAULT TABLESPACE USERS
TEMPORARY TABLESPACE TEMP
PROFILE DEFAULT ACCOUNT UNLOCK;

ALTER USER ProjetSGBD QUOTA UNLIMITED ON USERS;

GRANT CONNECT TO ProjetSGBD;
GRANT RESOURCE TO ProjetSGBD;
GRANT EXECUTE ON SYS.DBMS_LOCK TO ProjetSGBD;
GRANT EXECUTE ON SYS.OWA_OPT_LOCK TO ProjetSGBD;
GRANT CREATE ANY DIRECTORY TO ProjetSGBD;

--Création du directory :

CREATE OR REPLACE DIRECTORY MYDIR AS 'C:\';
CREATE OR REPLACE DIRECTORY MYDIR AS 'D:\HEPL\B3\SGBD\Projet_final\Fichier_SGBD';

GRANT EXECUTE,READ, WRITE ON DIRECTORY MYDIR TO ProjetSGBD;

SELECT DIRECTORY_NAME, DIRECTORY_PATH
FROM ALL_DIRECTORIES
WHERE DIRECTORY_NAME = 'PROJETFINALDIR';

--Suppression du DIRECTORY :

DROP DIRECTORY MYDIR;

--Dans Ventes :
--DROP TABLES :

DROP TABLE Ventes;
DROP TABLE Articles;
DROP TABLE Clients;
DROP TABLE Magasins;
DROP TABLE Ventes_TE;

--Création de la table externe Ventes_TE :

CREATE TABLE Ventes_TE
(
    idVente NUMBER(2) NOT NULL,
    idClient NUMBER(2) NOT NULL,
    nomClient VARCHAR2(50) NOT NULL,
    prenomClient VARCHAR2(50) NOT NULL,
    emailClient VARCHAR2(50) NOT NULL,
    idMagasin NUMBER(2) NOT NULL,
    nomMagasin VARCHAR2(50) NOT NULL,
    codePostalMagasin NUMBER(4) NOT NULL,
    ListeAchatArticles VARCHAR2(4000),
    dateAchat DATE NOT NULL,
    URLTicket VARCHAR2(100) NOT NULL
)
ORGANIZATION EXTERNAL
(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY MYDIR
    ACCESS PARAMETERS
    (
        RECORDS DELIMITED BY NEWLINE
        CHARACTERSET AL32UTF8
        FIELDS TERMINATED BY ';'
        (
            idVente INTEGER EXTERNAL(2),
            idClient INTEGER EXTERNAL(2),
            nomClient CHAR(50),
            prenomClient CHAR(50),
            emailClient CHAR(50),
            idMagasin INTEGER EXTERNAL(2),
            nomMagasin CHAR(50),
            codePostalMagasin INTEGER EXTERNAL(4),
            ListeAchatArticles VARCHAR(4000),
            dateAchat CHAR(10) DATE_FORMAT DATE MASK "dd/mm/yyyy",
            URLTicket CHAR(100)
        )
    )
    LOCATION ('Ventes.txt')
)
REJECT LIMIT UNLIMITED;

SELECT * FROM Ventes_TE;

--Création des différentes tables :

CREATE TABLE Articles
(
    idArticle NUMBER(2) NOT NULL,
    desriptionArticle VARCHAR(100) NOT NULL,
    prix FLOAT NOT NULL,
    quantite NUMBER(2) NOT NULL,
    PRIMARY KEY(idArticle)
);

CREATE TABLE Clients
(
    idClient NUMBER(2) NOT NULL,
    nomClient VARCHAR(30) NOT NULL,
    prenomClient VARCHAR(30) NOT NULL,
    emailClient VARCHAR(50) NOT NULL,
    PRIMARY KEY(idClient)
);

CREATE TABLE Magasins
(
    idMagasin NUMBER(2) NOT NULL,
    nomMagasin VARCHAR(50) NOT NULL,
    codePostalMagasin NUMBER(4) NOT NULL,
    PRIMARY KEY(idMagasin)
);

CREATE TABLE Ventes
(
    idVente NUMBER(2) NOT NULL,
    idArticleV NUMBER(2) NOT NULL,
    idClientV NUMBER(2) NOT NULL,
    idMagasinV NUMBER(2) NOT NULL,
    dateAchat DATE NOT NULL,
    URLTicket VARCHAR(100) NOT NULL,
    PRIMARY KEY(idVente),
    FOREIGN KEY (idArticleV) REFERENCES Articles(idArticle),
    FOREIGN KEY (idClientV) REFERENCES Clients(idClient),
    FOREIGN KEY (idMagasinV) REFERENCES Magasins(idMagasin)
);

--Insertion des données dans les tables :

INSERT INTO Clients (idClient,nomClient,prenomClient,emailClient)
SELECT DISTINCT idClient,nomClient,prenomClient,emailClient
FROM Ventes_TE;

INSERT INTO Magasins (idMagasin,nomMagasin,codePostalMagasin)
SELECT DISTINCT idMagasin,nomMagasin,codePostalMagasin
FROM Ventes_TE;

INSERT INTO Ventes(idVente,idArticleV,idClientV,idMagasinV,dateAchat,URLTicket)
SELECT DISTINCT idVente,idArticle,idClient,idMagasin,dateAchat,URLTicket
FROM Ventes_TE;

--Ici, on normalise les données des articles et on les mets dans une table temporaire :

CREATE TABLE Article_tmp
SELECT idVente, REGEXP_SUBSTR(str, '[^.]+', 1, 1) as idArticle,
REGEXP_SUBSTR(str, '[^.]+', 1, 2) as desriptionArticle,
REGEXP_SUBSTR(str, '[^.]+', 1, 3) as prix,
REGEXP_SUBSTR(str, '[^.]+', 1, 4) as quantite
FROM
(
    SELECT distinct idVente, trim(regexp_substr(str, '[^&]+', 1,
    level)) str
    FROM (SELECT idVente, ListeAchatArticles str FROM Ventes_TE) t
    CONNECT BY instr(str, '&', 1, level - 1) > 0
    order by idVente
);

--Ajout des données des articles contenus dans la table temporaire dans la table Articles :

INSERT INTO Articles(idArticle,desriptionArticle,prix,quantite)
SELECT DISTINCT idArticle,desriptionArticle,prix,quantite
FROM Article_tmp;

--Gestion des BLOBS :

--Fragmentation dans une BDD distribuée :
--Code postal compris entre 0 et 4999 :

CREATE TABLE Ventes0A4999 AS
SELECT * FROM Ventes
WHERE idMagasinV IN 
(
    SELECT idMagasin FROM Magasins
    WHERE codePostalMagasin BETWEEN 0 AND 4999
);

--Code postal compris entre 5000 et 9999 :

CREATE TABLE Ventes5000A9999
SELECT * FROM Ventes
WHERE idMagasinV IN 
(
    SELECT idMagasin FROM Magasins
    WHERE codePostalMagasin BETWEEN 5000 AND 9999
);

--Création du service REST :
--Méthode GET :

SELECT * FROM Ventes INNER JOIN Articles ON (Ventes.idArticleV = Articles.idArticle)
INNER JOIN Clients ON (Ventes.idClientV = Clients.idClient)
INNER JOIN Magasins ON (Ventes.idMagasinV = Magasins.idMagasin);

--Méthode POST :

CREATE OR REPLACE PROCEDURE UPDATE_CODE_POSTAL (
    idMagasinChangeant IN Magasins.idMagasin%TYPE,
    codePostalMagasinModifie IN Magasins.codePostalMagasin%TYPE
) AS
    idMagasinModifie IN Magasins.idMagasin%TYPE;
    nouveauCodePostalMagasin IN Magasins.codePostalMagasin%TYPE;
BEGIN
    --On vérifie dans quelle tranche se trouve le nouveau code postal afin de soit le copier dans l'autre BDD :

    IF (nouveauCodePostalMagasin BETWEEN 0 AND 4999 AND codePostalMagasinModifie BETWEEN 5000 AND 9999) THEN
        DELETE FROM Ventes0A4999 WHERE idMagasin = idMagasinModifie;
        
        INSERT INTO Ventes5000A9999(idVente,idArticleV,idClientV,idMagasinV,dateAchat,URLTicket)
        VALUES (idVente,idArticleV,idClientV,idMagasinModifie,dateAchat,URLTicket)
        WHERE idMagasinV = idMagasinModifie;

        UPDATE Magasins
        SET codePostalMagasin = nouveauCodePostalMagasin
        WHERE idMagasin = idMagasinModifie;

    ELSE IF (nouveauCodePostalMagasin BETWEEN 5000 AND 9999 AND codePostalMagasinModifie BETWEEN 0 AND 4999) THEN
        DELETE FROM Ventes5000A9999 WHERE idMagasin = idMagasinModifie;
        
        INSERT INTO Ventes0A4999(idVente,idArticleV,idClientV,idMagasinV,dateAchat,URLTicket)
        VALUES (idVente,idArticleV,idClientV,idMagasinModifie,dateAchat,URLTicket)
        WHERE idMagasinV = idMagasinModifie;

        UPDATE Magasins
        SET codePostalMagasin = nouveauCodePostalMagasin
        WHERE idMagasin = idMagasinModifie;

    ELSE
    
        UPDATE Magasins
        SET codePostalMagasin = nouveauCodePostalMagasin
        WHERE idMagasin = idMagasinModifie;
    END IF;
    
    COMMIT;
END;

/*CREATE TABLE Ventes_TE
(
    idVente NUMBER(2) NOT NULL,
    idClient NUMBER(2) NOT NULL,
    nomClient VARCHAR(30) NOT NULL,
    prenomClient VARCHAR(30) NOT NULL,
    emailClient VARCHAR(50) NOT NULL,
    idMagasin NUMBER(2) NOT NULL,
    nomMagasin VARCHAR(30) NOT NULL,
    codePostalMagasin NUMBER(4) NOT NULL,
    ListeAchatArticles CLOB NOT NULL,
    dateAchat DATE NOT NULL,
    URLTicket VARCHAR(100) NOT NULL
)
ORGANIZATION EXTERNAL
(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY MYDIR
    ACCESS PARAMETERS
    (
        --commentaires
        RECORDS DELIMITED BY NEWLINE
        characterset "AL32UTF8"
        FIELDS TERMINATED BY ';'
        LOBFILE (ListeAchats) TERMINATED BY '&'
        MISSING FIELD VALUES ARE NULL
        (
            idVente unsigned integer external(2),
            idClient unsigned integer external(2),
            nomClient char(30),
            prenomClient char(30),
            emailClient char(50),
            idMagasin unsigned integer external(2),
            nomMagasin char(30),
            codePostalMagasin unsigned integer external(4),
            ListeAchatArticles char(4000),
            dateAchat char(9) date_format date mask "dd/mm/yy",
            URLTicket char(100)
        )
    )
    LOCATION('C:\Users\Utilisateur\Desktop\Document_avec_enregistrements\Ventes.txt')
)
REJECT LIMIT UNLIMITED;*/

/*

CREATE USER MesVentes
IDENTIFIED BY MesVentes
DEFAULT TABLESPACE USERS
TEMPORARY TABLESPACE TEMP
PROFILE DEFAULT ACCOUNT UNLOCK;
ALTER USER MesVentes QUOTA UNLIMITED ON USERS;
GRANT CONNECT TO MesVentes;
GRANT RESOURCE TO MesVentes;
GRANT CREATE SESSION TO MesVentes;
GRANT ALTER SESSION TO MesVentes;
GRANT CREATE TABLE TO MesVentes;
GRANT CREATE PROCEDURE TO MesVentes;
GRANT CREATE SEQUENCE TO MesVentes;
GRANT CREATE TRIGGER TO MesVentes;
GRANT EXECUTE ON SYS.DBMS_LOCK TO MesVentes;
GRANT EXECUTE ON SYS.OWA_OPT_LOCK TO MesVentes;

orcl

SELECT DIRECTORY_NAME, DIRECTORY_PATH
FROM ALL_DIRECTORIES
WHERE DIRECTORY_NAME = 'MYDIR';

*/
