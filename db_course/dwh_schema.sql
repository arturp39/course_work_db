-- Drop existing tables if they exist
DROP TABLE IF EXISTS Fact_Sales CASCADE;
DROP TABLE IF EXISTS Fact_Payment CASCADE;
DROP TABLE IF EXISTS Dim_Customer CASCADE;
DROP TABLE IF EXISTS Dim_Product CASCADE;
DROP TABLE IF EXISTS Dim_Time CASCADE;
DROP TABLE IF EXISTS Dim_Order CASCADE;

-- Create Dimension Tables
CREATE TABLE Dim_Customer (
    CustomerID BIGSERIAL PRIMARY KEY,
    FirstName TEXT,
    LastName TEXT,
    Email TEXT,
    Address TEXT,
    StartDate DATE,
    EndDate DATE,
    IsCurrent BOOLEAN
);

CREATE TABLE Dim_Product (
    ProductID BIGSERIAL PRIMARY KEY,
    Name TEXT,
    Category TEXT,
    Price DECIMAL(10, 2)
);

CREATE TABLE Dim_Time (
    Date DATE PRIMARY KEY,
    Month INT,
    Quarter INT,
    Year INT
);

CREATE TABLE Dim_Order (
    OrderID BIGSERIAL PRIMARY KEY,
    CustomerID BIGINT,
    OrderDate DATE,
    TotalAmount DECIMAL(10, 2)
);

-- Create Fact Tables
CREATE TABLE Fact_Sales (
    SalesID BIGSERIAL PRIMARY KEY,
    OrderID BIGINT,
    ProductID BIGINT,
    CustomerID BIGINT,
    Date DATE,
    Quantity_Sold INT,
    Total_Sales DECIMAL(10, 2),
    FOREIGN KEY (ProductID) REFERENCES Dim_Product(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES Dim_Customer(CustomerID),
    FOREIGN KEY (Date) REFERENCES Dim_Time(Date),
    FOREIGN KEY (OrderID) REFERENCES Dim_Order(OrderID),
    CONSTRAINT unique_order_product UNIQUE (OrderID, ProductID)
);

CREATE TABLE Fact_Payment (
    PaymentID BIGSERIAL PRIMARY KEY,
    OrderID BIGINT,
    CustomerID BIGINT,
    Date DATE,
    Amount DECIMAL(10, 2),
    FOREIGN KEY (OrderID) REFERENCES Dim_Order(OrderID),
    FOREIGN KEY (CustomerID) REFERENCES Dim_Customer(CustomerID),
    FOREIGN KEY (Date) REFERENCES Dim_Time(Date)
);

-- Indexes for improved query performance
CREATE INDEX idx_fact_sales_product ON Fact_Sales(ProductID);
CREATE INDEX idx_fact_sales_customer ON Fact_Sales(CustomerID);
CREATE INDEX idx_fact_sales_order ON Fact_Sales(OrderID);

-- Analytical Queries

-- Total Sales by Product
SELECT dp.Name AS Product_Name, SUM(fs.Total_Sales) AS Total_Sales
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.ProductID = dp.ProductID
GROUP BY dp.Name
ORDER BY Total_Sales DESC;

-- Sales by Month
SELECT dt.Month, dt.Year, SUM(fs.Total_Sales) AS Total_Sales
FROM Fact_Sales fs
JOIN Dim_Time dt ON fs.Date = dt.Date
GROUP BY dt.Month, dt.Year
ORDER BY dt.Year, dt.Month;

-- Top Selling Products
SELECT dp.Name AS Product_Name, SUM(fs.Quantity_Sold) AS Total_Quantity_Sold
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.ProductID = dp.ProductID
GROUP BY dp.Name
ORDER BY Total_Quantity_Sold DESC
LIMIT 10;

-- Sales by Category
SELECT dp.Category, SUM(fs.Total_Sales) AS Total_Sales
FROM Fact_Sales fs
JOIN Dim_Product dp ON fs.ProductID = dp.ProductID
GROUP BY dp.Category
ORDER BY Total_Sales DESC;


select * from Fact_Payment
select * from dim_customer
select * from dim_order
select * from dim_product
select * from dim_time
select * from fact_sales

