Cliente postgress -> psql -U postgres
Crear la base de datos -> CREATE DATABASE underwear;
Conectarse a la base de datos -> \c underwear
Listar las bases de datos -> \l
Ver las tablas -> \dt
Salir de la base de datos -> \q
postgres=# drop DATABASE underwear;



"CustomerID","CustomerName","Region","Country","PriceCategory","CustomerClass","LeadSource","Discontinued"
1,"C1","Moscow","Russian Federation",1,"Large-Scale Wholesaler-1","Referral by the Central Office",0

CREATE TABLE customers (
CustomerID SERIAL PRIMARY KEY,
CustomerName VARCHAR(100),
Region VARCHAR(100),
Country VARCHAR(100),
PriceCategory INTEGER,
CustomerClass VARCHAR(100),
LeadSource VARCHAR(100),
Discontinued INTEGER
);

"EmployeeID","EmployeeName"
1,"E1"

CREATE TABLE employees (
EmployeeID SERIAL PRIMARY KEY,
EmployeeName VARCHAR(4)
);

"ShippingMethodID","ShippingMethod"
"1","Ex Works"

CREATE TABLE shipping_methods (
ShippingMethodID SERIAL PRIMARY KEY,
ShippingMethod VARCHAR(20)
);



"OrderID","CustomerID","EmployeeID","ShippingMethodID","OrderDate","ShipDate","FreightCharge"
"2","1","1","1","7/10/2003","7/10/2003","0.00"

CREATE TABLE orders (
OrderID SERIAL PRIMARY KEY,
CustomerID INTEGER REFERENCES customers(CustomerID) ON DELETE CASCADE,
EmployeeID INTEGER REFERENCES employees(EmployeeID) ON DELETE CASCADE,
ShippingMethodID VARCHAR(4),
OrderDate DATE,
ShipDate DATE,
FreightCharge VARCHAR(8)
);


"PaymentMethodID","PaymentMethod"
"1","Cash"

CREATE TABLE payment_methods (
PaymentMethodID SERIAL PRIMARY KEY,
PaymentMethod VARCHAR(15)
);

"PaymentID","OrderID","PaymentMethodID","PaymentDate","PaymentAmount"
"1","2","1","7/10/2003","603.50"

CREATE TABLE payments (
PaymentID SERIAL PRIMARY KEY,
OrderID INTEGER REFERENCES orders(OrderID) ON DELETE CASCADE,
PaymentMethodID INTEGER REFERENCES payment_methods(PaymentMethodID) ON DELETE CASCADE,
PaymentDate DATE,
PaymentAmount VARCHAR(10)
);

"ProductID","ProductName","Color","ModelDescription","FabricDescription","Category","Gender","ProductLine",
"Weight","Size","PackSize","Status","InventoryDate","PurchasePrice"

"1","3-182","","AT","182","Undershirts","Girls' Undershirts","Underwear","822","3","Dozen","In Production","7/10/2003","6.60"
CREATE TABLE products (
ProductID SERIAL PRIMARY KEY,
ProductName VARCHAR(4),
Color VARCHAR(10),
ModelDescription VARCHAR(10),
FabricDescription VARCHAR(10), 
Category VARCHAR(25),
Gender VARCHAR(50),
ProductLine VARCHAR(25),
Weight VARCHAR(5),
Size VARCHAR(4),
PackSize VARCHAR(4),
Status VARCHAR(30),
InventoryDate DATE,
PurchasePrice VARCHAR(10)
);

"SupplierID","SupplierName"
"1","S1"
CREATE TABLE supliers (
SupplierID SERIAL PRIMARY KEY,
SupplierName VARCHAR(30)
);




"PurchaseOrderID","SupplierID","EmployeeID","ShippingMethodID","OrderDate"
"25","1","1","3","10/15/2003"

CREATE TABLE purchase_order (
PurchaseOrderID SERIAL PRIMARY KEY,
SupplierID INTEGER REFERENCES supliers(SupplierID) ON DELETE CASCADE,
EmployeeID INTEGER REFERENCES employees(EmployeeID) ON DELETE CASCADE,
ShippingMethodID VARCHAR(10), 
OrderDate DATE
);





"TransactionID","ProductID","PurchaseOrderID","MissingID","TransactionDate","UnitPurchasePrice","QuantityOrdered",
"QuantityReceived","QuantityMissing"
"1","1","","","5/29/2003","","28","28",""

CREATE TABLE inventory_transactions (
TransactionID SERIAL PRIMARY KEY,
ProductID INTEGER REFERENCES products(ProductID) ON DELETE CASCADE,
PurchaseOrderID INTEGER REFERENCES purchase_order(PurchaseOrderID) ON DELETE CASCADE,
MissingID VARCHAR(30),
TransactionDate DATE,
UnitPurchasePrice VARCHAR(40),
QuantityOrdered VARCHAR(4),
QuantityReceived VARCHAR(30),
QuantityMissing VARCHAR(20)
);


"OrderDetailID","OrderID","ProductID","QuantitySold","UnitSalesPrice"
"2","2","955","5","7.50"

CREATE TABLE order_details (
OrderDetailID SERIAL PRIMARY KEY,
OrderID INTEGER REFERENCES orders(OrderID) ON DELETE CASCADE,
ProductID INTEGER REFERENCES products(ProductID) ON DELETE CASCADE,
QuantitySold VARCHAR(7),
UnitSalesPrice VARCHAR(10)
);