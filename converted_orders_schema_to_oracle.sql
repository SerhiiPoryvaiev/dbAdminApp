CREATE TABLE mysql_orders (
    order_id NUMBER(10) NOT NULL,
    user_id NUMBER(10),
    customer_id NUMBER(10),
    product_id NUMBER(10),
    order_date DATE NOT NULL,
    quantity NUMBER(10) NOT NULL,
    total_price NUMBER(10, 2) NOT NULL
);

