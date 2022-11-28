from mrjob.job import MRJob
from mrjob.step import MRStep
# importing MRJob and MRStep from mrjob package 

# Defining a class named "RatingsBreakdown" that inherits the capabilites of "MRJob"
class RatingsBreakdown(MRJob):
    # Define single mapreduce phase
    def steps(self):
      return [
            # The mapper function of that step is defined at "mapper_get_ratings" and reducer function is "reducer_count_ratings" and those are the two pieces of information the MRStep needs to work.
            MRStep(mapper=self.mapper_get_ratings,
            reducer=self.reducer_count_ratings)
    ]
# Define mapper to break up lines of data on tab, extracts the rating and return key value pair of rating and 1
    def mapper_get_ratings(self, _, line):
      (userID, movieID, rating, timestamp) = line.split('\t')
      yield rating, 1
# Reduce key values, return rating and rating count.
# Reducer function takes in eacn indvidual unique rating one through five and a list of all the one values associated with each rating and then it yields the key which is the rating (1 through 5) and the sum of all those ones which is eventually the count of each rating type.
    def reducer_count_ratings(self, key, values):
      yield key, sum(values)
if __name__ == '__main__':
    RatingsBreakdown.run()
