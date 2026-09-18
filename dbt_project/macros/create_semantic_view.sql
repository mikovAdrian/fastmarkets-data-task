{#
  Creates the semantic view that Cortex Analyst reads.

  Why a macro rather than a model: a dbt model is a SELECT statement, and a
  semantic view is a schema object with its own DDL - there is nothing for dbt
  to materialize. Keeping it here means it is still version-controlled and
  still derives its schema from the dbt target, rather than living in a SQL
  file someone runs by hand against whatever database they happen to be in.

      dbt run-operation create_semantic_view

  The view sits on top of the marts, not on the raw or staging layer. That is
  deliberate: the semantic layer should expose the modelled star schema, so a
  question answered in natural language and a question answered in SQL go
  through the same definitions of revenue, week and customer.
#}

{% macro create_semantic_view() %}

  {% set marts = target.database ~ '.' ~ target.schema ~ '_marts' %}

  {% set sql %}
CREATE OR REPLACE SEMANTIC VIEW {{ marts }}.SV_ORDER_SALES
  TABLES (
    customers AS {{ marts }}.DIM_CUSTOMER
      PRIMARY KEY (customer_id)
      WITH SYNONYMS ('clients', 'buyers')
      COMMENT = 'One row per customer',
    orders AS {{ marts }}.FCT_ORDER
      PRIMARY KEY (order_id)
      WITH SYNONYMS ('purchases')
      COMMENT = 'One row per order header',
    items AS {{ marts }}.FCT_ORDER_ITEM
      PRIMARY KEY (order_item_id)
      WITH SYNONYMS ('order lines', 'line items')
      COMMENT = 'One row per order line - the lowest grain in the model'
  )
  RELATIONSHIPS (
    items_to_orders AS items (order_id) REFERENCES orders,
    orders_to_customers AS orders (customer_id) REFERENCES customers
  )
  FACTS (
    items.quantity AS quantity,
    items.unit_price AS unit_price,
    items.line_total AS line_total,
    orders.order_total AS order_total
  )
  DIMENSIONS (
    items.product_id AS product_id
      WITH SYNONYMS ('product', 'sku')
      COMMENT = 'Product identifier: A1, B1 or C1',
    items.product_name AS product_name
      WITH SYNONYMS ('product name')
      COMMENT = 'Widget, Gadget or Doohickey',
    items.week_start AS week_start
      WITH SYNONYMS ('week', 'week beginning', 'week commencing')
      COMMENT = 'Monday of the week the order falls in',
    items.order_date AS order_date
      WITH SYNONYMS ('date', 'order day')
      COMMENT = 'Date the order was placed',
    customers.customer_name AS customer_name
      WITH SYNONYMS ('client name')
      COMMENT = 'Customer name as sent by the source',
    customers.has_valid_contact AS has_valid_contact
      WITH SYNONYMS ('reachable', 'contactable')
      COMMENT = 'True when the customer has a valid phone or a valid email'
  )
  METRICS (
    items.total_revenue AS SUM(items.line_total)
      WITH SYNONYMS ('revenue', 'sales', 'turnover', 'money')
      COMMENT = 'Sum of quantity times unit price. The revenue definition used
                 for the top-seller flag, so natural language and SQL agree',
    items.total_quantity AS SUM(items.quantity)
      WITH SYNONYMS ('units', 'volume', 'quantity sold')
      COMMENT = 'Units sold. Deliberately separate from revenue: the two
                 metrics disagree on the top seller in 80 of the 144 weeks',
    items.line_count AS COUNT(items.order_item_id)
      WITH SYNONYMS ('lines', 'order lines')
      COMMENT = 'Number of order lines',
    orders.order_count AS COUNT(orders.order_id)
      WITH SYNONYMS ('orders', 'number of orders')
      COMMENT = 'Number of orders',
    customers.customer_count AS COUNT(customers.customer_id)
      WITH SYNONYMS ('customers', 'number of customers')
      COMMENT = 'Number of customers'
  )
  COMMENT = 'Semantic layer over the order star schema, read by Cortex Analyst'
  {% endset %}

  {% do log('Creating semantic view ' ~ marts ~ '.SV_ORDER_SALES', info=True) %}
  {% do run_query(sql) %}
  {% do log('Done.', info=True) %}

{% endmacro %}
