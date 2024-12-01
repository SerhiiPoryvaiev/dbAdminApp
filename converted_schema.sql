-- Converted database schema from MySQL to Oracle
CREATE TABLE holidays (
    HOLIDAY_ID NUMBER(10, 2) NOT NULL,
    HOLIDAY_NAME VARCHAR2(100),
    HOLIDAY_DATE TIMESTAMP
);

CREATE TABLE mysql_customers (
    customer_id NUMBER(10) NOT NULL,
    customer_name VARCHAR2(100) NOT NULL,
    contact_number VARCHAR2(15),
    address CLOB
);

CREATE TABLE mysql_orders (
    order_id NUMBER(10) NOT NULL,
    user_id NUMBER(10),
    customer_id NUMBER(10),
    product_id NUMBER(10),
    order_date DATE NOT NULL,
    quantity NUMBER(10) NOT NULL,
    total_price NUMBER(10, 2) NOT NULL
);

CREATE TABLE mysql_products (
    product_id NUMBER(10) NOT NULL,
    product_name VARCHAR2(100) NOT NULL,
    price NUMBER(10, 2) NOT NULL,
    stock_quantity NUMBER(10) NOT NULL
);

CREATE TABLE mysql_users (
    user_id NUMBER(10) NOT NULL,
    username VARCHAR2(50) NOT NULL,
    email VARCHAR2(100) NOT NULL,
    password VARCHAR2(255) NOT NULL
);


