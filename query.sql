CREATE TABLE SalesOrders(
        OrderId INT,
		OrderDate DATE,
		CustomerID INT,
		CustomerName VARCHAR(200),
		CustomerEmail VARCHAR(200),
		Country VARCHAR(100),
		ProductID INT,
		ProductCategory VARCHAR(100),
		ProductName VARCHAR(100),
		Quantity INT,
		UnitPrice DECIMAL(18,2),
		TotalPrice DECIMAL(18,2),
		SalesRegion VARCHAR(50)
);

SELECT * FROM SalesOrders;