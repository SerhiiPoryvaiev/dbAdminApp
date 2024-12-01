-- Schema for table holidays
CREATE TABLE holidays (
    HOLIDAY_ID decimal(10,2) NOT NULL,
    HOLIDAY_NAME varchar(100),
    HOLIDAY_DATE datetime
);

-- Schema for table mysql_customers
CREATE TABLE mysql_customers (
    customer_id int(11) NOT NULL,
    customer_name varchar(100) NOT NULL,
    contact_number varchar(15),
    address text
);

-- Schema for table mysql_orders
CREATE TABLE mysql_orders (
    order_id int(11) NOT NULL,
    user_id int(11),
    customer_id int(11),
    product_id int(11),
    order_date date NOT NULL,
    quantity int(11) NOT NULL,
    total_price decimal(10,2) NOT NULL
);

-- Schema for table mysql_products
CREATE TABLE mysql_products (
    product_id int(11) NOT NULL,
    product_name varchar(100) NOT NULL,
    price decimal(10,2) NOT NULL,
    stock_quantity int(11) NOT NULL
);

-- Schema for table mysql_users
CREATE TABLE mysql_users (
    user_id int(11) NOT NULL,
    username varchar(50) NOT NULL,
    email varchar(100) NOT NULL,
    password varchar(255) NOT NULL
);

