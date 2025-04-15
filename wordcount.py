from pyspark import SparkContext

# Create SparkContext
sc = SparkContext("local", "WordCountApp")

# Read text file
text_file = sc.textFile("sample.txt")

# Word count logic
word_counts = (
    text_file.flatMap(lambda line: line.split(" "))      # Split lines into words
             .map(lambda word: (word, 1))                 # Map each word to (word, 1)
             .reduceByKey(lambda a, b: a + b)             # Reduce by key (word)
)

# Show result
for word, count in word_counts.collect():
    print(f"{word}: {count}")
