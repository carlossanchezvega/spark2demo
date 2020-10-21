from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,round
import configparser as cp, sys

# we load the config parser
props = cp.RawConfigParser()
props.read('src/main/resources/application.properties')
env = sys.argv[1]



# firstly, we have to create the session object
# In development, we might want to develop using a local cluster, whereas in production we will use yarn
# we load the environment from the properties file
spark = SparkSession.builder.appName('Get daily product revenue').master(props.get(env, 'executionMode')).getOrCreate()
spark.conf.set('spark.sql,shuffle.partitions','2')
spark.sparkContext.setLogLevel('ERROR')
inputBaseDir = props.get(env, 'input.base.dir')

# In case we hadn't a properties file
#spark = SparkSession.builder.appName('Get daily product revenue').master('local').getOrCreate()

orders = spark.read. \
    format('csv'). \
    schema('order_id int, order_date string, order_customer_id int, order_status string'). \
    load(inputBaseDir+'/orders')

#load('/public/retail_db/orders')

orderItems = spark.read. \
    format('csv'). \
    schema('''order_item_id int, order_item_order_id int, order_item_product_id int,order_item_quantity float, order_item_subtotal float,order_item_product_price float''').\
    load(inputBaseDir+'/order_items')

#    load('/public/retail_db/order_items''')

dailyProductRevenue = orders.\
                      where(orders.order_status.isin('COMPLETE', 'CLOSED')).\
                      join(orderItems, orders.order_id == orderItems.order_item_order_id). \
                      groupBy('order_date', 'order_item_product_id'). \
                      agg(round(sum('order_item_subtotal'),2).alias('product_revenue'))


dailyProductRevenueSorted = dailyProductRevenue.\
    orderBy(dailyProductRevenue.orderDate, dailyProductRevenue.revenue.desc())


outputBaseDir = props.get(env, 'output.base.dir')

dailyProductRevenueSorted.\
    write.\
    csv(outputBaseDir + '/daily_product_revenue')

