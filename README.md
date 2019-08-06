# yelp-stars
Use PySpark to analyze Yelp Database

The goal is to compute the number of businesses, total review count, and average star rating for each neighborhood in each city. If a business has multiple neighborhoods, its review count and stars should be attributed to all of the neighborhoods. If the neighborhoods list is empty, then we will use 'Unknown' as the name of the neighborhood.

The final result should be a TSV file.

In this desired output file, each row contains 5 columns, which are separated by a tab. For example, this row
Ann Arbor Downtown Ann Arbor 273 7137 3.66117216117
means the neighborhood of “Downtown Ann Arbor” in the city of “Ann Arbor” has 273 businesses, and their total review count is 7137, and their average star rating is 3.66.

The rows in the output file should be sorted in alphabetical order of the city names, and the neighborhoods in each city are sorted by the number of businesses in decreasing order.

