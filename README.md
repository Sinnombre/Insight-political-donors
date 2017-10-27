Evan Miller

Entry for Insight Data Science Challenge

find-political-donors


# Usage:

This code was written in C#, using Visual Studio. The program should work as presented. The run.sh shell script should as well, but I am less certain about this, since I made it in Windows, where shell scripts exhibit very different behavior. Note that as C# is a compiled language, the first line of run.sh runs the compiler; please remove this line if grading based on run-time.

This code uses an open source implementation of priority queues, written by Alexey Kurakin and available under The Code Project Open License. Details can be found at:

https://www.codeproject.com/Articles/126751/Priority-queue-in-C-with-the-help-of-heap-data-str

License available here:

https://www.codeproject.com/info/cpol10.aspx

For portability reasons, the open source code was simply added to the bottom of a single file with the code I wrote. Obviously in a real production, I would link a Nuget package, but that wouldn't be portable to compiling on a Unix box...



# Explanation:

In the main routine, the input file is iterated over line-by-line (simulating a data-streaming problem). First the line is parsed into an instance of the Line class, which pulls out the relevant information and marks any data that is missing. Assuming OTHER_ID is blank, a C_ID has been found and the amount of the contribution was parsable and non-negative, the code tries to process the line by zip code and by date.

Each candidate has a dictionary keyed by the zip codes they have received contributions from. The objects in this dictionary are instances of the CandidateZip class, which tracks the total count and sum of contributions from the zip code to the candidate, and also stores the values of the contributions for computing the median. The values are stored using a double-heap structure. Basically, there is a left max-heap, containing values smaller than the median, and a right min-heap, containing values larger than it. Because the left heap is a max-heap, its largest value sits on top. The right heap is a min-heap, so its root is its smallest value. The current median either sits at the root of the left heap (for odd counts) or is the average of the left and the right heap roots (for even counts). Whenever a new value is added, it is first placed on the left heap. For odd increments, this means that the left heap now has one more element than the right heap. If the root of the left heap is bigger than the root of the right heap, they are swapped (and heap adjustments take place as needed), ensuring that the true median is the root of the left heap. For even increments, after adding the new value to the left heap, the left heap is now two bigger than the right heap; to rebalance, the root of the left heap is simply taken and placed in the right heap.

Anyway, the upshot of all this is that insertions happen in O(log(n)) time. Additionally, looking up the median is trivial, since it only cares about the roots of the two heaps; this happens in O(1) time. Similar time complexity could be achieved with a single well-constructed binary tree, but the code required would be far more complicated (it would have to remain perfectly symmetric).

After processing the zip component of the new line, the code write the output to the file. Again, this mimics a data streaming problem, where outputs are written as inputs come in. This will likely be the slowest part of the code; more efficiency could be achieved by batching the outputs before each write.

Since the by-date output is only written once, after reading the whole input, that code is much simpler. Each instance of the candidate class has a dictionary keyed by date containing a list of the amounts they received on that day. As a new line is read, the amount is simply added to the list, an O(1) operation.

After the read loop finishes, the code take the list of candidates and sorts them alphabetically. For each candidate, the dates they received contributions are then sorted chronologically. For each candidate-date combination, the contributions they received are finally sorted by value. This makes it trivial to pick the medians out of the middle of the lists. Each of these three sorts are O(n*log(n)) operations, done using C#'s native list.sort() method (usually quicksort).



# Assumptions:
My code requires the following to be true, or it will likely fail; usually, this will mean not including certain lines of data, but in some cases the code will crash:
a) All lines must have at least 15 pipes (the '|' character) (another catch could be added to check for badly formatted input lines, but I chose not to for efficiency).
b) The inputs must be in the correct order (at least the ones we care about).
c) ZIP codes must be at least 5 characters long, numeric and non-negative. Zip codes violating this will still be used for by-date calculations; zip codes longer than 5 are fine but will be truncated to their first 5 characters.
d) I use the default date as a catch, so contributions made on 1/1/0001 will not be accurately recorded.
e) I use negative amounts as a catch, so any negative contribution (if their system tracks expenses that way or something) will be ignored. Contributions of zero dollars are allowed.
f) I assume " " (i.e. whitespace) is a valid OTHER_ID and will not include rows with that OTHER_ID in calculations.



# Complexity:

The time complexity for this code is usually going to be O(n*log(n)). For the by-zip section, reading each line requires writing to a heap, so at most O(log(n)) time is spent every one of the n cycles (assuming the priority queue library I used was well designed). Reading the medians, as mentioned above, is an O(1) operation, as are all of the various dictionary lookups that get done.

For the by-date section, the read operation only adds to the end of a list (note that c# lists are not to be confused with linked lists, and that we never remove information). Before the write operation, however, we do a number of sorts. Because there are at most n objects to be sorted, this will in general take n*log(n) time. However, in the worst case (there is only one candidate who got all there contributions on one day and they were ordered in the worst possible way), it could in theory take O(n^2) time, so the worst case run time complexity for the entire code is O(n^2).

In terms of storage complexity, it is O(n). In fact, I am storing the exact value of every contribution twice (once by date and once by zip). I suppose I could only save everything once by being extraordinarily clever, but doing so would likely add massive time complexity. I could also reduce the storage requirements by changing the format of the C_IDs (they all seem to be integers led by a C) and dates (simply count days from some arbitrary date; no need to track times).



# Improvements:

I could use the double-heap storage system for by-date too. The advantage would be that the worst-case time complexity would be only O(n*log(n)), and that I would be using the same structures and methods for both routines. The disadvantage would be that that method is far more complex and difficult to explain, and that I would be front-loading the computations into the data-streaming phase of the program.

I could also use a custom heap architecture to simplify some of the processes. For instance, when the heaps swap roots on even counts, we know that the value being added to the left heap will be its new largest value. However, doing this would add a HUGE amount of coding complexity, and open the doors for thousands of new errors to crop up. I cannot imagine a situation where I would want to do that for this task.

Finally, if the quicksorts really do become a problem, I could do a random sampling of the by-date lists first. This would still get a decently-accurate median value, at a tiny fraction of the computational cost. I could even randomly sample entries from the data stream in the first place, if I wanted to avoid the space complexity problem.
