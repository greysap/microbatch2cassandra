CREATE TABLE transaction_aggregates (
    customer_id int,
    window text,
    transaction_type text,
    total_amount double,
    PRIMARY KEY (customer_id, window, transaction_type)
);