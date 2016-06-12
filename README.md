# TDT4305:

My rather informal homework for TDT4305.   
While imperfect, I uploaded this to benefit the pyspark community (theres not enough pyspark code in the wild).
Also, I kind of like my own psuedo-TDD approach.  See *run_tasks.sh* for examples.

The report/documentation was not that important, so its not written formally.
It does contain comparisons on the running times of caching/broadcasting, which could be interesting.
See *./doc/build/report.pdf*.


### Lesson Learnt:
I did my work in three iterations; I started with a small input, then a larger one, 
before I iterated over the complete dataset. This was a mistake. 
I recommend sampling the dataset, but also trying out your algorithm on large
inputs right away. You'll learn that e.g. caching is less effective for large datasets.
