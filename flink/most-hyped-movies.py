from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor, DataTypes, Schema, FormatDescriptor

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

td = TableDescriptor.for_connector("filesystem")\
    .schema(Schema.new_builder()
            .column("userID", DataTypes.INT())
            .column("movieID", DataTypes.INT())
            .column("rating", DataTypes.INT())
            .column("timestamp", DataTypes.STRING())
            .build())\
    .option("path", "/files/u.data")\
    .format(FormatDescriptor.for_format("csv").option("field-delimiter", "\t").build())\
    .build()

table_env.create_temporary_table("ratings", td)
ratings = table_env.from_path("ratings")

td2 = TableDescriptor.for_connector("filesystem")\
    .schema(Schema.new_builder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .build())\
    .option("path", "/files/u-mod.item")\
    .format(FormatDescriptor.for_format("csv").option("field-delimiter", "|").build())\
    .build()

table_env.create_temporary_table("movies", td2)
movies = table_env.from_path("movies")

grouped_5_stars = ratings.filter(ratings.rating == 5).group_by(ratings.movieID).select(ratings.movieID, ratings.rating.count.alias("count_most_hyped"))
sorted = grouped_5_stars.order_by(grouped_5_stars.count_most_hyped.desc).fetch(10)
joined = sorted.join(movies)\
    .where(sorted.movieID == movies.id)\
    .select(sorted.movieID, movies.name, sorted.count_most_hyped)


grouped = ratings.group_by(ratings.movieID).select(ratings.movieID.alias("movID"), ratings.rating.count.alias("count_rating"))
joined_count_rating = joined.join(grouped)\
    .where(grouped.movID == joined.movieID)\
    .select(joined.movieID, movies.name, joined.count_most_hyped, (joined.count_most_hyped / grouped.count_rating.cast(DataTypes.FLOAT())).alias("pomer"))

joined_count_rating_ordered = joined_count_rating.order_by(joined_count_rating.pomer.desc)


otd = TableDescriptor.for_connector("print")\
    .schema(Schema.new_builder()
            .column("movieID", DataTypes.INT())
            .column("movieName", DataTypes.STRING())
            .column("Reviewed by 5", DataTypes.BIGINT())
            .column("Procent", DataTypes.FLOAT())
            .build())\
    .build()

table_env.create_temporary_table("sink", otd)
joined_count_rating_ordered.execute_insert("sink").wait()

