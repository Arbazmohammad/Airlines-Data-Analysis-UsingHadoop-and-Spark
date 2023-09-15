Solving using PySpark:-


from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Airlines Data Analysis").getOrCreate()

# Define the file paths
input_path = "/home/arbazmoham18edu/Input/"

airports_file = input_path + "airports_mod.dat"
airlines_file = input_path + "Final_airlines.dat"
routes_file = input_path + "routes.dat"

# Read data from the three files into Spark DataFrames
airports_df = spark.read.csv(airports_file, header=True, inferSchema=True, sep=",")
airlines_df = spark.read.csv(airlines_file, header=True, inferSchema=True, sep=",")
routes_df = spark.read.csv(routes_file, header=True, inferSchema=True, sep=",")

# Showing the schema and a sample of the DataFrames
print("Airports Data:")
airports_df.printSchema()
airports_df.show(5)

print("Airlines Data:")
airlines_df.printSchema()
airlines_df.show(5)

print("Routes Data:")
routes_df.printSchema()
routes_df.show(5)


# Problem Statement A - find the list of airports operating in the country India.
# Filter the data to select airports in India
indian_airports_df = airports_df.filter(airports_df["Country"] == "India")

# Select the desired columns (AirportID, Name, City)
result_df = indian_airports_df.select("AirportID", "Name", "City")

# Show the result
result_df.show(truncate=False)


# Problem Statement B - Find the list of airlines having zero stopes
# Perform an inner join between airlines and routes on AirlineID
joined_df = airlines_df.join(routes_df, ["AirlineID"], "inner")

# Filter the joined DataFrame to find airlines with zero stops
zero_stops_df = joined_df.filter(joined_df["Stops"] == 0)

# Select the desired columns (AirlineID, Name) from the filtered DataFrame
result_df = zero_stops_df.select("AirlineID", "Name").distinct()

# Show the result
result_df.show(truncate=False)


# Problem Statement C - List of Airlines operating with code share 'Y'
# Perform an inner join between airlines and routes on AirlineID
joined_df = airlines_df.join(routes_df, ["AirlineID"], "inner")

# Filter the joined DataFrame to find airlines with codeshare = 'Y'
codeshare_airlines_df = joined_df.filter(joined_df["Codeshare"] == 'Y')

# Select the desired columns (AirlineID, Name) from the filtered DataFrame
result_df = codeshare_airlines_df.select("AirlineID", "Name").distinct()

# Show the result
result_df.show(truncate=False)


# Problem Statement D - Finding Country or Territory having highest Airports
# Group the DataFrame by "Country" and count the number of airports in each country
country_airports_count_df = airports_df.groupBy("Country").count()

# Find the country with the highest number of airports
max_airports_country = country_airports_count_df.orderBy(col("count").desc()).first()

# Show the result
print("Country with the Highest Number of Airports:")
print("Country:", max_airports_country["Country"])
print("Number of Airports:", max_airports_country["count"])


# Problem Statement D - Find the List of Active Airlines in the United States
# Filter the DataFrame to select active airlines in the United States
active_us_airlines_df = airlines_df.filter((airlines_df["Country"] == "United States") & (airlines_df["Active"] == "Y"))

# Select the desired columns (AirlineID and Name)
result_df = active_us_airlines_df.select("AirlineID", "Name")

# Show the result
result_df.show(truncate=False)

# Stop the Spark session
spark.stop()