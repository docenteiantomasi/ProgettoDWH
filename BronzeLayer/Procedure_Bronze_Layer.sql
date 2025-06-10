/*Creazione del Bronze Layer*/
CREATE DATABASE BronzeLayer

USE BronzeLayer

/*Creazione delle tabelle del Bronze Layer*/
CREATE TABLE dbo.Orders(
RowID nvarchar(255),
OrderID nvarchar(255),
OrderDate nvarchar(255),
ShipDate nvarchar(255),
ShipMode nvarchar(255),
CustomerID nvarchar(255),
CustomerName nvarchar(255),
Segment nvarchar(255),
Country nvarchar(255),
City nvarchar(255),
State nvarchar(255),
PostalCode nvarchar(255),
Region nvarchar(255),
ProductID nvarchar(255),
Category nvarchar(255),
SubCategory nvarchar(255),
ProductName nvarchar(255),
Sales nvarchar(255),
Quantity nvarchar(255),
Discount nvarchar(255),
Profit nvarchar(255));

CREATE TABLE Returns(
	Returned nvarchar(255),
	OrderID nvarchar(255));


/*Caricamento tramite bulk insert*/
BULK INSERT dbo.Orders
FROM 'C:\Users\ianto\Desktop\Nuovo\Diagramma a stella\Superstore_orders.csv'
WITH (
	ROWTERMINATOR = '\n',
	FIELDTERMINATOR = ';',
	FIRSTROW = 2,
	MAXERRORS = 0);

BULK INSERT dbo.Returns
FROM 'C:\Users\ianto\Desktop\Nuovo\Diagramma a stella\Superstore_returns.csv'
WITH (
	ROWTERMINATOR = '\n',
	FIELDTERMINATOR = ';',
	FIRSTROW = 2,
	MAXERRORS = 0);



USE BronzeLayer
CREATE or ALTER PROCEDURE dbo.CaricaBronzeLayer
AS
BEGIN
	TRUNCATE TABLE dbo.Orders;
	TRUNCATE TABLE dbo.Returns;

	BULK INSERT dbo.Orders
	FROM 'C:\Users\ianto\Desktop\Nuovo\Diagramma a stella\Superstore_orders.csv'
	WITH (
		ROWTERMINATOR = '\n',
		FIELDTERMINATOR = ';',
		FIRSTROW = 2,
		MAXERRORS = 0,
		FORMAT = 'CSV',--aggiunta successiva per gestire il carattere qualificatore
		CODEPAGE  = '65001'); --aggiunta successiva per gestire la codifica del file

	BULK INSERT dbo.Returns
	FROM 'C:\Users\ianto\Desktop\Nuovo\Diagramma a stella\Superstore_returns.csv'
	WITH (
		ROWTERMINATOR = '\n',
		FIELDTERMINATOR = ';',
		FIRSTROW = 2,
		MAXERRORS = 0,
		FORMAT = 'CSV', /*aggiunta successiva per gestire il carattere qualificatore*/
		CODEPAGE  = '65001'); /*aggiunta successiva per gestire la codifica del file*/
END

/*Test della procedura*/
USE BronzeLayer
EXEC DBO.CaricaBronzeLayer

SELECT * FROM BronzeLayer.dbo.Orders --where rowid=3447


SELECT * FROM BronzeLayer.dbo.Returns

/*Creazione del Silver Layer*/
CREATE DATABASE SilverLayer;

use SilverLayer;

/*Creazione della tabella del Silver Layer*/
CREATE TABLE dbo.Orders(
RowID INT NOT NULL PRIMARY KEY,
OrderID nvarchar(255) NOT NULL,
OrderDate DATE NOT NULL,
ShipDate DATE NOT NULL,
ShipMode nvarchar(255),
CustomerID nvarchar(255) NOT NULL,
CustomerName nvarchar(255) NOT NULL,
Segment nvarchar(255),
Country nvarchar(255),
City nvarchar(255),
State nvarchar(255),
PostalCode nvarchar(255),
Region nvarchar(255),
ProductID nvarchar(255) NOT NULL,
Category nvarchar(255),
SubCategory nvarchar(255),
ProductName nvarchar(255),
Sales DECIMAL(18,4) NOT NULL,
Quantity DECIMAL(18,4) NOT NULL ,
Discount DECIMAL(18,4) NOT NULL,
Profit DECIMAL(18,4) NOT NULL
);

CREATE UNIQUE INDEX ix_chiave_naturale ON dbo.Orders(OrderID,ProductID);

/*Popolamento della tabella degli ordini*/
INSERT INTO SilverLayer.dbo.Orders(
	RowID,OrderID,OrderDate,ShipDate,ShipMode,CustomerID,CustomerName,Segment,
	Country,City,State,PostalCode ,Region,ProductID,Category, SubCategory, ProductName ,
	Sales, Quantity, Discount, Profit)
SELECT 	o.RowID, 
	o.OrderID,
	convert(date,o.OrderDate,103) as OrderDate,
	convert(date,o.ShipDate,103) as ShipDate,
	o.ShipMode,o.CustomerID,o.CustomerName,o.Segment,
	o.Country,o.City,o.State,o.PostalCode ,o.Region,o.ProductID,o.Category, 
	o.SubCategory, o.ProductName ,
	convert(decimal(18,4),Replace(o.Sales,',','.')) as Sales, 
	convert(decimal(18,4),Replace(o.Quantity,',','.')) as Quantity,
	convert(decimal(18,4),Replace(o.Discount,',','.')) as Discount,
	convert(decimal(18,4),Replace(o.Profit,',','.')) as Profit
FROM BronzeLayer.dbo.Orders as o
LEFT JOIN BronzeLayer.dbo.returns as r
	on o.OrderID = r.OrderID
WHERE r.OrderID IS NULL
/* codice per individuare righe con problemi di conversione*/
WHERE TRY_convert(decimal(18,4),Replace(Sales,',','.')) IS NULL
AND Sales IS NOT NULL*/


/*Codice per individuare coppie orderid, productid duplicate */
select orderid, productid,count(*)
FROM BronzeLayer.dbo.Orders
group by orderid, productid
having count(*)>1

/*Procedura per automatizzare il caricamento*/
use silverlayer
create procedure dbo.CaricaSilverLayer
as 
begin
	truncate table SilverLayer.dbo.Orders;

	WITH CTE AS (
		SELECT 	o.RowID, 
			o.OrderID,
			convert(date,o.OrderDate,103) as OrderDate,
			convert(date,o.ShipDate,103) as ShipDate,
			o.ShipMode,o.CustomerID,o.CustomerName,o.Segment,
			o.Country,o.City,o.State,o.PostalCode ,o.Region,o.ProductID,o.Category, 
			o.SubCategory, o.ProductName ,
			convert(decimal(18,4),Replace(o.Sales,',','.')) as Sales, 
			convert(decimal(18,4),Replace(o.Quantity,',','.')) as Quantity,
			convert(decimal(18,4),Replace(o.Discount,',','.')) as Discount,
			convert(decimal(18,4),Replace(o.Profit,',','.')) as Profit,
			rank() over(partition by o.OrderID,o.ProductID order by o.RowID desc) AS rn
		FROM BronzeLayer.dbo.Orders as o
		LEFT JOIN BronzeLayer.dbo.returns as r
			on o.OrderID = r.OrderID
		WHERE r.OrderID IS NULL)
	INSERT INTO SilverLayer.dbo.Orders(
		RowID,OrderID,OrderDate,ShipDate,ShipMode,CustomerID,CustomerName,Segment,
		Country,City,State,PostalCode ,Region,ProductID,Category, SubCategory, ProductName ,
		Sales, Quantity, Discount, Profit)
	SELECT RowID,OrderID,OrderDate,ShipDate,ShipMode,CustomerID,CustomerName,Segment,
		Country,City,State,PostalCode ,Region,ProductID,Category, SubCategory, ProductName ,
		Sales, Quantity, Discount, Profit
	FROM CTE 
	WHERE  rn = 1;

end

/*Test delle procedure*/
use silverlayer
exec dbo.CaricaSilverLayer


USE BronzeLayer
EXEC DBO.CaricaBronzeLayer

use silverlayer
exec dbo.CaricaSilverLayer


/*Creazione del Database GoldLayer */
CREATE DATABASE GoldLayer

use GoldLayer

/*Prima versione della DimCustomer
CREATE TABLE dbo.DimCustomer(
	CustomerID int not null primary key identity(1,1),
	CustomerIDOrigin varchar(255) not null,	
	CustomerName  varchar(255) not null,
	Segment varchar(255),
	CityOrigin varchar(255)  not null,
	Country varchar(255) not null,
	State varchar(255),
	PostalCode varchar(255),	
	Region varchar(255));
*/

/*Versione successiva della DimCustomer*/
CREATE TABLE dbo.DimCustomer(
	CustomerID int not null primary key identity(1,1),
	CustomerIDOrigin varchar(255) not null,	
	CustomerName  varchar(255) not null,
	Segment varchar(255));

create unique index chiave_naturale on dbo.DimCustomer(CustomerIDOrigin);
	
/*Modifica successiva per avere compatibilità con il silver layer*/
ALTER TABLE dbo.DimCustomer ALTER COLUMN CustomerName  Nvarchar(255) not null


/*Creazione successiva della tabella DimCity*/
CREATE TABLE dbo.DimCity(
	CityID int not null primary key identity(1,1),
	CityOrigin varchar(255)  not null,
	Country varchar(255) not null,
	State varchar(255),
	PostalCode varchar(255),	
	Region varchar(255));

/*Prima chiave naturale candidata
create unique index chiave_naturale on dbo.DimCity(CityOrigin); 
*/

/*Modifica successiva per gestire i duplicati*/
--drop index chiave_naturale on dbo.DimCity;
alter table dbo.DimCity alter column PostalCode varchar(255) not null
create unique index chiave_naturale on dbo.DimCity(CityOrigin, PostalCode);


/*Creazione DimProduct*/
CREATE TABLE dbo.DimProduct(
ProductID int not null primary key identity(1,1),
ProductIDOrigin varchar(255) not null,
Category varchar(255) not null,
SubCategory varchar(255) not null,
ProductName varchar(255) not null)

create unique index chiave_naturale on dbo.DimProduct(ProductIDOrigin);

/*Creazione DimDate*/
CREATE TABLE dbo.DimDate(
IdDate int not null primary key,
Date DATE not null,
Giorno int not null,
Mese int not null,
Anno int not null)

create unique index chiave_naturale on dbo.DimDate(Date);

/*Creazone FactSales, prima versione
create table dbo.FactSales(
IdSales  int not null primary key identity(1,1),
OrderID varchar(255) not null,
ProductID int not null,
OrderDate int not null,
CustomerID int not null,
ShipDate int not null,
ShipMode varchar(255) null,
Sales	decimal(18,4),
Quantity decimal(18,4),
Discount decimal(18,4),
Profit decimal(18,4),
foreign key (ProductID) References DBO.DimProduct(ProductID),
foreign key (OrderDate) References DBO.DimDate(IdDate),
foreign key (ShipDate) References DBO.DimDate(IdDate),
foreign key (CustomerId) References DBO.DimCustomer(CustomerID)
)

create unique index chiave_naturale on dbo.FactSales(OrderID, ProductID);
*/


/*Creazone FactSales, versione successiva con chiave esterna a DimCity*/
create table dbo.FactSales(
IdSales  int not null primary key identity(1,1),
OrderID varchar(255) not null,
ProductID int not null,
OrderDate int not null,
CustomerID int not null,
CityID int not null,
ShipDate int not null,
ShipMode varchar(255) null,
Sales	decimal(18,4),
Quantity decimal(18,4),
Discount decimal(18,4),
Profit decimal(18,4),
foreign key (ProductID) References DBO.DimProduct(ProductID),
foreign key (OrderDate) References DBO.DimDate(IdDate),
foreign key (ShipDate) References DBO.DimDate(IdDate),
foreign key (CustomerId) References DBO.DimCustomer(CustomerID),
foreign key (CityID) References DBO.DimCity(CityID),
);

create unique index chiave_naturale on dbo.FactSales(OrderID, ProductID);

/*Codice per cancellare dati da DimCustomer*/
--DELETE FROM GoldLayer.dbo.DimCustomer

/*Codice per popolare la DimCustomer, prima versione
INSERT INTO GoldLayer.dbo.DimCustomer(
	CustomerIDOrigin,CustomerName,Segment,City,Country,State,PostalCode,Region)
SELECT DISTINCT	CustomerID,CustomerName,Segment,City,Country,State,PostalCode,Region
FROM SilverLayer.dbo.Orders 
*/

/*Individuiamo i clienti con city differenti*/
SELECT CustomerID,
	COUNT(DISTINCT City)
FROM SilverLayer.dbo.Orders
GROUP BY CustomerID
HAVING COUNT(DISTINCT City)>1


/*Codice per popolare la DimCustomer, versione successiva dopo le modifiche
allo star schema*/
INSERT INTO GoldLayer.dbo.DimCustomer(
	CustomerIDOrigin,CustomerName,Segment)
SELECT DISTINCT	CustomerID,CustomerName,Segment
FROM SilverLayer.dbo.Orders

/*Codice per popolare la DimCity*/
INSERT INTO GoldLayer.DBO.DimCity(
	CityOrigin,	Country,State ,	PostalCode,	Region)
SELECT DISTINCT City,	Country,State ,	PostalCode,	Region
FROM silverlayer.dbo.Orders

SELECT * FROM  GoldLayer.DBO.DimCity

/*Individuiamo le città con stati differenti*/
SELECT City,count(distinct State)
FROM SilverLayer.dbo.Orders
group by City
having count(distinct State) > 1

/*Individuiamo le coppie città-state con postalcode differenti*/
SELECT City,state,count(distinct PostalCode)
FROM SilverLayer.dbo.Orders
group by City,state
having count(distinct PostalCode) > 1

/*Verifichiamo che City, PostalCode è chiave naturale*/
SELECT City,PostalCode,count(distinct state)
FROM SilverLayer.dbo.Orders
group by City,PostalCode
having count(distinct state) > 1

/*Inserimento dei dati nella DimProduct, prima versione
INSERT INTO GoldLayer.dbo.DimProduct(
	ProductIDOrigin,Category,SubCategory,ProductName)
SELECT DISTINCT
	ProductID,Category,SubCategory,ProductName
FROM SilverLayer.dbo.Orders

*/

/*Individuiamo righe duplicate*/
select *
from GoldLayer.dbo.DimProduct
where ProductIDOrigin = 'FUR-BO-10002213'

SELECT ProductID,COUNT(DISTINCT ProductName)
FROM SilverLayer.dbo.Orders
GROUP BY ProductID
HAVING COUNT(DISTINCT ProductName)>1

/*Inserimento dati nella DimProduct, versione successiva*/
INSERT INTO GoldLayer.dbo.DimProduct(
	ProductIDOrigin,Category,SubCategory,ProductName)
SELECT 
	ProductID,Category,SubCategory,MAX(ProductName) AS ProductName
FROM SilverLayer.dbo.Orders
GROUP BY ProductID,Category,SubCategory

/*Popolamento una tantum della DimDate*/
DECLARE @StartDate DATE = '2000-01-01';
DECLARE @EndDate DATE = '2050-12-31';
DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO dbo.DimDate(
		IdDate, Date, Giorno, Mese, Anno)
    VALUES ( CAST(YEAR(@CurrentDate) * 10000 + MONTH(@CurrentDate) * 100 + DAY(@CurrentDate) AS INT), 
			@CurrentDate, 
			DAY(@CurrentDate), 
			MONTH(@CurrentDate), 
			YEAR(@CurrentDate));
    
    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate);
END;

select *
from GoldLayer.dbo.DimDate

/*Codice per svuotare la tabella FactSales*/
--TRUNCATE TABLE GoldLayer.dbo.FactSales

/*Popolamento della FactSales*/
INSERT INTO GoldLayer.dbo.FactSales(
OrderID,ProductID,OrderDate,CustomerID ,CityID,
ShipDate ,ShipMode ,Sales,Quantity,Discount ,Profit)
SELECT 
O.OrderID,
p.ProductID,
d1.IdDate as OrderDate,
cu.CustomerID ,
ci.CityID,
d2.IdDate as ShipDate ,
o.ShipMode ,
o.Sales,
o.Quantity,
o.Discount ,
o.Profit 
FROM SilverLayer.dbo.Orders as o
LEFT JOIN GoldLayer.dbo.DimProduct as p
	on o.ProductID = p.ProductIDOrigin
left join GoldLayer.dbo.DimDate as d1
	on o.OrderDate = d1.Date
left join GoldLayer.dbo.DimDate as d2
	on o.ShipDate = d2.Date
left join goldLayer.dbo.DimCustomer as cu
	on o.CustomerID = cu.CustomerIDOrigin
left join GoldLayer.dbo.DimCity as ci
	on o.City = ci.CityOrigin
	and o.PostalCode = ci.PostalCode

/*Esempio di Join tra tabella dei fatti e delle dimensioni*/
SELECT TOP 10 *
FROM GoldLayer.dbo.FactSales AS FS
LEFT JOIN GoldLayer.DBO.DimCustomer AS CU
	ON FS.CustomerID = CU.CustomerID

/*Creazione delle procedure per automatizzare i caricamenti*/
USE GoldLayer
CREATE or alter PROCEDURE dbo.CaricaDimCustomer AS 
	set nocount on
BEGIN	
	
	create table #customer(
			CustomerIDOrigin varchar(255) not null,	
			CustomerName  nvarchar(255) not null,
			Segment varchar(255) )
	
	insert into #customer(
		CustomerIDOrigin,CustomerName,Segment)
	SELECT DISTINCT	CustomerID AS CustomerIdOrigin,CustomerName,Segment
	FROM SilverLayer.dbo.Orders


	UPDATE DC
	SET DC.CustomerName = Cu.CustomerName,
		DC.segment = cu.Segment
	FROM GoldLayer.DBO.DimCustomer as DC
	INNER JOIN #Customer AS Cu
		on DC.CustomerIdOrigin = Cu.CustomerIdOrigin
	WHERE EXISTS (SELECT DC.CustomerName, DC.segment
						EXCEPT
				  SELECT Cu.CustomerName, cu.Segment)

	INSERT INTO GoldLayer.DBO.DimCustomer(
		CustomerIdOrigin,CustomerName,segment)
	select CustomerIdOrigin,CustomerName,segment 
	from #Customer as cu
	WHERE NOT EXISTS (SELECT *
					  FROM GoldLayer.DBO.DimCustomer AS Du
					  WHERE  Du.CustomerIdOrigin = Cu.CustomerIdOrigin)
		
END
use GoldLayer
exec  dbo.CaricaDimCustomer 

/*Analisi di clienti con caratteri particolari nel nome*/
select * from GoldLayer.DBO.DimCustomer where customeridorigin='AH-10690'
select * from SilverLayer.dbo.Orders where customerid='AH-10690'


/*Procedura per caricare DimCity*/
use GoldLayer
CREATE PROCEDURE dbo.CaricaDimCity as 
set nocount on
begin

	SELECT DISTINCT City,	Country,State ,	PostalCode,	Region
	into #city
	FROM silverlayer.dbo.Orders

	update dc
	set	dc.Country = cy.Country,
	dc.State = cy.State,
	dc.Region = cy.Region
	from GoldLayer.dbo.DimCity as  dc
	inner join #city as cy
		on dc.CityOrigin = cy.city
		and dc.PostalCode = cy.PostalCode
	where exists (select dc.Country,dc.State,dc.Region
					except
				  select  cy.Country,cy.State,cy.Region)

	insert into GoldLayer.dbo.DimCity(
		CityOrigin,	Country,State ,	PostalCode,	Region)
	select city,	Country,State ,	PostalCode,	Region
	from #city as cy
	where not exists (select *
					  from GoldLayer.dbo.DimCity as dc
					  where dc.CityOrigin = cy.city
						and dc.PostalCode = cy.PostalCode)
end

use GoldLayer
exec  dbo.CaricaDimCity 


/*Procedura per caricare DimProduct*/
CREATE PROCEDURE dbo.CaricaDimProduct as 
set nocount on
begin


	SELECT 
		ProductID,Category,SubCategory,MAX(ProductName) AS ProductName
	into #products
	FROM SilverLayer.dbo.Orders
	GROUP BY ProductID,Category,SubCategory

	update dp
	set	dp.Category = pr.Category,
	dp.SubCategory = pr.SubCategory,
	dp.ProductName = pr.ProductName
	from GoldLayer.dbo.DimProduct as  dp
	inner join #products as pr
		on dp.ProductIDOrigin = pr.ProductID
	where exists (select dp.Category,dp.SubCategory,dp.ProductName
					except
				  select  pr.Category,pr.SubCategory,pr.ProductName)

	INSERT INTO GoldLayer.dbo.DimProduct(
	ProductIDOrigin,Category,SubCategory,ProductName)
	select ProductID,Category,SubCategory,ProductName
	from #products as pr
	where not exists (select *
					  from GoldLayer.dbo.DimProduct as dp
					  where pr.ProductID = dp.ProductIDOrigin
						)
end

use GoldLayer
exec dbo.CaricaDimProduct

/*Procedura per caricare FactSales*/
use GoldLayer
create procedure dbo.caricafactsales as 
set nocount on
begin
	INSERT INTO GoldLayer.dbo.FactSales(
	OrderID,ProductID,OrderDate,CustomerID ,CityID,
	ShipDate ,ShipMode ,Sales,Quantity,Discount ,Profit)
	SELECT 
	O.OrderID,
	p.ProductID,
	d1.IdDate as OrderDate,
	cu.CustomerID ,
	ci.CityID,
	d2.IdDate as ShipDate ,
	o.ShipMode ,
	o.Sales,
	o.Quantity,
	o.Discount ,
	o.Profit 
	FROM SilverLayer.dbo.Orders as o
	LEFT JOIN GoldLayer.dbo.DimProduct as p
		on o.ProductID = p.ProductIDOrigin
	left join GoldLayer.dbo.DimDate as d1
		on o.OrderDate = d1.Date
	left join GoldLayer.dbo.DimDate as d2
		on o.ShipDate = d2.Date
	left join goldLayer.dbo.DimCustomer as cu
		on o.CustomerID = cu.CustomerIDOrigin
	left join GoldLayer.dbo.DimCity as ci
		on o.City = ci.CityOrigin
		and o.PostalCode = ci.PostalCode

end

exec dbo.caricafactsales


/*Test delle procedure per un nuovo file*/
USE BronzeLayer
EXEC DBO.CaricaBronzeLayer
SELECT * FROM BronzeLayer.DBO.ORDERS

use SilverLayer
exec dbo.CaricaSilverLayer
SELECT * FROM SilverLayer.DBO.ORDERS

use GoldLayer
exec dbo.CaricaDimCity
SELECT * FROM GoldLayer.dbo.DimCity

exec dbo.CaricaDimCustomer
SELECT * FROM GoldLayer.dbo.DimCustomer where CustomerIDOrigin = 'CG-12520'

exec dbo.CaricaDimProduct
SELECT * FROM GoldLayer.dbo.DimProduct

exec dbo.caricafactsales
SELECT count(*) FROM GoldLayer.dbo.FactSales