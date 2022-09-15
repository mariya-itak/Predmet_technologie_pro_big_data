from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, TableConfig, DataTypes, Schema
from pyflink.common import Row
from pyflink.table.udf import udf


env_settings = EnvironmentSettings.in_batch_mode()
#env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

td = TableDescriptor.for_connector("filesystem") \
    .schema(Schema.new_builder() \
        .column("customer_id", DataTypes.INT()) \
        .column("product_id", DataTypes.INT()) \
        .column("price", DataTypes.FLOAT()) \
        .build())\
    .option("path", "customer-orders.csv") \
    .format("csv") \
    .build()

table_env.create_temporary_table('source', td)
tab = table_env.from_path('source')


def print_result(table):
    with table.execute().collect() as result:
        for row in result:
            print(row)


def round_function(a: Row) -> Row:
    return Row(a[0], round(a[1], 2))

result = tab\
    .group_by(tab.customer_id) \
    .select(tab.customer_id, tab.price.sum.alias("total_spent"))\

#pro prepnuti do stream mode staci vymenit prvni radek za streaming_mode(), zakomentovat radek se setridenim a upravit mapovani .map
result_ordered = result.order_by(result.total_spent.desc)
func = udf(round_function, result_type=DataTypes.ROW(
                                     [DataTypes.FIELD("customer_id", DataTypes.INT()),
                                      DataTypes.FIELD("total_spent", DataTypes.FLOAT())]))
table = result_ordered.map(func).alias('customer_id', 'total_spent')
print_result(table)
