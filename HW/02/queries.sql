use classicmodels;

-- QUERY 1: Count the number of employees whose last name or first name starts
-- 			with the letter 'P'.
SELECT COUNT(*) 
FROM employees
WHERE LOWER(firstName) LIKE 'P%' OR LOWER(lastName) LIKE 'P%';

-- QUERY 2: For how many letters of the alphabet are there more than one employee 
-- 			whose last name starts with that letter? Hint: The substr function will be 
-- 			useful here in a GROUP BY clause.
SELECT SUBSTR(UPPER(lastName),1,1) AS letter, COUNT(*) AS employeeCount
FROM employees
GROUP BY letter
HAVING employeeCount > 1
ORDER BY employeeCount DESC;

-- QUERY 3: How many orders have not yet shipped?
SELECT COUNT(*) AS Orders_Not_Shipped
FROM orders
WHERE LOWER(status) = "in process";

-- QUERY 4: How many orders where shipped less than 2 days before they were required?
SELECT COUNT(*) AS Orders_Count
FROM orders
WHERE requiredDate-shippedDate<2;

-- QUERY 5: For each distinct product line, what is the total dollar value 
-- 			of orders placed?
SELECT prd_tbl.productLine, SUM(ord_tbl.priceEach*ord_tbl.quantityOrdered) AS Total_Dollar_Value
FROM orderdetails AS ord_tbl JOIN products AS prd_tbl
	ON ord_tbl.productCode=prd_tbl.productCode
GROUP BY prd_tbl.productLine
ORDER BY Total_Dollar_Value DESC;

-- QUERY 6: For the first three customers in alphabetal order by name, what is the 
-- 			name of every product they have ordered?
-- SOLUTION PROCESS: 
-- 			Step 1: Find first 3 customers sorted by name in the customers table
-- 			Step 2: Find all orders placed by these customers
-- 			Step 3: Find product codes for all these orders
-- 			Step 4: Find product names for all these product codes
-- 			Step 5: Project the customer name, product code, product name tuples
DROP TEMPORARY TABLE cust_order;
CREATE TEMPORARY TABLE cust_order (
	customerName VARCHAR(50), 
    orderNumber INT(11)
);

INSERT INTO cust_order 
(
SELECT cust.customerName, orders.orderNumber
FROM orders JOIN ( SELECT DISTINCT customerNumber, customerName
				FROM customers 
                ORDER BY customerName LIMIT 3) AS cust
	ON cust.customerNumber=orders.customerNumber);

SELECT * FROM cust_order;
    
SELECT DISTINCT cust_order.customerName, orderdetails.productCode, products.productName
FROM orderdetails JOIN cust_order
	ON orderdetails.orderNumber=cust_order.orderNumber
    JOIN products ON orderdetails.productCode=products.productCode;
