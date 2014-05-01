create table customers(account_id bigint,
                       customer_id bigint,
                       name text,
                       primary key (account_id, customer_id));

create table orders(customer_id bigint,
                    ordered_at timestamp,
                    order_id bigint,
                    primary key(customer_id, ordered_at, order_id))