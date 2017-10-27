using System;
using System.Collections.Generic;
using System.Linq;
using System.Collections;

namespace Find_Donors
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Count() != 3)
            {
                Console.WriteLine(args);
                Console.WriteLine("Dude, where's my inputs?");
                Console.Read();
                return;
            }

            Dictionary<string, Candidate> Candidates = new Dictionary<string, Candidate>();     // List of valid candidates
            string line_string;                                                                 // Temp holder for each line
            Line line = new Line();                                                             // Temp holder for line object
            CandidateZip ztemp;
            List<float> atemp;

            System.IO.StreamReader input_file = new System.IO.StreamReader(args[0]);      //(@"C:\Users\Evan\Documents\01 Actual Docs\Job Applications Stuff\Insight 2017\find-political-donors-master\input\itcont.txt");
            System.IO.StreamWriter zip_file = new System.IO.StreamWriter(args[1]);        //(@"C:\Users\Evan\Documents\01 Actual Docs\Job Applications Stuff\Insight 2017\find-political-donors-master\output\medianvals_by_zip.txt");

            while ((line_string = input_file.ReadLine()) != null)           // Interate over the strings, treating the input as a data stream
            {
                // Parse line
                line.ParseLine(line_string);

                // If not individual, skip
                if (!String.IsNullOrEmpty(line.Other_ID)) { continue; }

                // If CMTE_ID or TRANSACTION_AMT invalid, skip
                if (line.C_ID == null || line.Amount < 0) { continue; }

                // If zip valid, process row for zip
                if(line.Zip5 >= 0)
                {
                    if (!Candidates.Keys.Contains(line.C_ID)) { Candidates.Add(line.C_ID, new Candidate()); }       // Add candidate if they don't exist yet
                    Candidates[line.C_ID].Process_LineByZip(line);

                    // Write streaming output
                    ztemp = Candidates[line.C_ID].Zips[line.Zip5];
                    zip_file.WriteLine("{0}|{1}|{2}|{3}|{4}", line.C_ID, line.Zip5.ToString("D5"), ztemp.PeekMedian(), ztemp.Transactions_Count, Math.Round(ztemp.Total_Amount));
                }

                // If date valid, process row for date
                if (line.T_Date != new DateTime())
                {
                    if (!Candidates.Keys.Contains(line.C_ID)) { Candidates.Add(line.C_ID, new Candidate()); }       // Add candidate if they don't exist yet
                    Candidates[line.C_ID].Process_LineByDate(line);
                }
            }

            // Close files
            input_file.Close();
            zip_file.Close();

            // Write by-date output
            System.IO.StreamWriter date_file = new System.IO.StreamWriter(args[2]);                   //(@"C:\Users\Evan\Documents\01 Actual Docs\Job Applications Stuff\Insight 2017\find-political-donors-master\output\medianvals_by_date.txt");
            List<string> CList = Candidates.Keys.ToList();          // Get candidates into a list
            List<DateTime> DList;
            CList.Sort();                                           // Sort list (to alphabetize candidates)
            int median;
            int mid;
            string DateFormatString = "MMddyyyy";                   // Format of FEC dates
            foreach ( string c in CList)                            // Interate over candidates
            {
                DList = Candidates[c].Dates.Keys.ToList();          // Get the dates this candidate has recieved contributions into a list
                DList.Sort();                                       // Sort the list
                foreach( DateTime d in DList)                       // Iterate over dates the candidate has recieved money
                {
                    atemp = Candidates[c].Dates[d];
                    atemp.Sort();                                   // Sort the amounts of money they recieved on this date (for finding median)
                    if(atemp.Count%2 == 0)                          // Find median contribution for this date
                    {
                        mid = atemp.Count / 2;
                        median = Convert.ToInt32(Math.Round((atemp[mid] + atemp[mid - 1]) / 2f));
                    }
                    else
                    {
                        median = Convert.ToInt32(Math.Round(atemp[(atemp.Count - 1) / 2]));
                    }
                    date_file.WriteLine("{0}|{1}|{2}|{3}|{4}", c, d.ToString(DateFormatString), median, atemp.Count(),Math.Round(atemp.Sum()));        // Write output
                }
            }
            date_file.Close();
        }
    }

    // The Line class parses a line in the input file from a string into to relevant data
    public class Line
    {
        public string C_ID;                         // The candidate id
        public int Zip5;                            // First five characters of zip code
        public float Amount;                        // Amount contributed
        public string Other_ID;                     // OTHER_ID
        public DateTime T_Date;                     // Date of contibution
        string DateFormatString = "MMddyyyy";       // Format of FEC dates

        // This function parses the input string. Data that is allowed to be bad (i.e. zip codes and dates) are placed within
        // try-catch statements
        public void ParseLine(string inp)
        {
            string[] values = inp.Split('|');

            C_ID = values[0];
            if (C_ID.Count() != 9) { C_ID = null; }
            try
            {

                Zip5 = int.Parse(values[10].Substring(0,5));
            }
            catch
            {
                Zip5 = -1;
            }

            try
            {
                T_Date = DateTime.ParseExact(values[13], DateFormatString, null);
            }
            catch { }

            try
            {
                Amount = float.Parse(values[14]);
            }
            catch
            {
                Amount = -1f;
            }
            Other_ID = values[15];
        }
    }

    // This class contains a single candidate, their contributions by zip, and their contributions by date
    public class Candidate
    {
        public Dictionary<int,CandidateZip> Zips = new Dictionary<int,CandidateZip>();                  // A dictionary of the zip codes contributions have come from
        public Dictionary<DateTime, List<float>> Dates = new Dictionary<DateTime, List<float>>();       // For each date, a list of the amounts they recieved that day

        // This function updates the Zips dictionary for the input recieved on a given line
        public void Process_LineByZip(Line input)
        {
            if (!Zips.Keys.Contains(input.Zip5)) { Zips.Add(input.Zip5, new CandidateZip()); }          // If no contributions have come from this zip before, create it
            Zips[input.Zip5].Add(input.Amount);                                                         // Update the candidate / zip object
        }

        // This function adds the amount contributed on a given line to the candidate-date list
        public void Process_LineByDate(Line input)
        {
            if (!Dates.Keys.Contains(input.T_Date)) { Dates.Add(input.T_Date, new List<float>()); }     // Add date to dictionary if this is the first contribution on that date
            Dates[input.T_Date].Add(input.Amount);                                                      // Add amount to list
        }
    }

    // For each candidate and zip code combination, this class manages the streaming median, as well as the
    //   total count and total amount of contributions. See README for a better description of the heaps used
    public class CandidateZip
    {
        public int Transactions_Count = 0;                                                  // Total number of contributions from this zip code to this candidate
        public float Total_Amount = 0;                                                      // Total amount of contributions from this zip code to this candidate

        public PriorityQueue<float,float> RightHeap = new PriorityQueue<float,float>();     // The larger half of contributions
        public PriorityQueue<float,float> LeftHeap = new PriorityQueue<float,float>();      // The smaller half of contributions

        // This function adds a new amount to the double-heap structure, in O(log(n)) time
        public void Add(float amount)
        {
            Transactions_Count = Transactions_Count + 1;        // Increment count
            Total_Amount = Total_Amount + amount;               // Increment amount

            LeftHeap.Enqueue(-amount, amount);                  // Add new amount to left heap, with priority inverse to its size
            if (Transactions_Count%2 == 0)                      // On even ticks, LeftHeap now has 2 more elements than RightHeap, so take its biggest element and give to right
            {
                float temp = LeftHeap.DequeueValue();
                RightHeap.Enqueue(temp, temp);
            }
            else
            {                                                   // On odd ticks, if the smallest element of right is smaller than the larger element of left, swap them
                if (Transactions_Count == 1) { return; }
                if ( RightHeap.PeekValue() < LeftHeap.PeekValue())
                {
                    float rightRoot = RightHeap.DequeueValue();
                    float leftRoot = LeftHeap.DequeueValue();
                    RightHeap.Enqueue(leftRoot, leftRoot);
                    LeftHeap.Enqueue(-rightRoot, rightRoot);
                }
            }
        }

        // This function finds the median of the contributions to a candidate from a zip code, in O(1) time
        public int PeekMedian()
        {
            float median;
            if(Transactions_Count == 0) { return 0; }                   // If no contributions have been made
            if(Transactions_Count%2 == 0)                               // If an even number of contributions have been made, average the roots of the two heaps
            {
                median = (RightHeap.PeekValue() + LeftHeap.PeekValue()) / 2.0f;
            }
            else
            {
                median = LeftHeap.PeekValue();                              // On odd numbers of contributions, the median is on the top of the left heap
            }
            return Convert.ToInt32(Math.Round(median));
        }
    }


    // ---------------------    END SUBMISSION CODE    -----------------------


    // --------------------- BEGIN OPEN-SOURCE PACKAGE -----------------------

/*
 * This code implements priority queue which uses min-heap as underlying storage
 * 
 * Copyright (C) 2010 Alexey Kurakin
 * www.avk.name
 * alexey[ at ]kurakin.me
 */



        /// <summary>
        /// Priority queue based on binary heap,
        /// Elements with minimum priority dequeued first
        /// </summary>
        /// <typeparam name="TPriority">Type of priorities</typeparam>
        /// <typeparam name="TValue">Type of values</typeparam>
        public class PriorityQueue<TPriority, TValue> : ICollection<KeyValuePair<TPriority, TValue>>
        {
            private List<KeyValuePair<TPriority, TValue>> _baseHeap;
            private IComparer<TPriority> _comparer;

            #region Constructors

            /// <summary>
            /// Initializes a new instance of priority queue with default initial capacity and default priority comparer
            /// </summary>
            public PriorityQueue()
                : this(Comparer<TPriority>.Default)
            {
            }

            /// <summary>
            /// Initializes a new instance of priority queue with specified initial capacity and default priority comparer
            /// </summary>
            /// <param name="capacity">initial capacity</param>
            public PriorityQueue(int capacity)
                : this(capacity, Comparer<TPriority>.Default)
            {
            }

            /// <summary>
            /// Initializes a new instance of priority queue with specified initial capacity and specified priority comparer
            /// </summary>
            /// <param name="capacity">initial capacity</param>
            /// <param name="comparer">priority comparer</param>
            public PriorityQueue(int capacity, IComparer<TPriority> comparer)
            {
                if (comparer == null)
                    throw new ArgumentNullException();

                _baseHeap = new List<KeyValuePair<TPriority, TValue>>(capacity);
                _comparer = comparer;
            }

            /// <summary>
            /// Initializes a new instance of priority queue with default initial capacity and specified priority comparer
            /// </summary>
            /// <param name="comparer">priority comparer</param>
            public PriorityQueue(IComparer<TPriority> comparer)
            {
                if (comparer == null)
                    throw new ArgumentNullException();

                _baseHeap = new List<KeyValuePair<TPriority, TValue>>();
                _comparer = comparer;
            }

            /// <summary>
            /// Initializes a new instance of priority queue with specified data and default priority comparer
            /// </summary>
            /// <param name="data">data to be inserted into priority queue</param>
            public PriorityQueue(IEnumerable<KeyValuePair<TPriority, TValue>> data)
                : this(data, Comparer<TPriority>.Default)
            {
            }

            /// <summary>
            /// Initializes a new instance of priority queue with specified data and specified priority comparer
            /// </summary>
            /// <param name="data">data to be inserted into priority queue</param>
            /// <param name="comparer">priority comparer</param>
            public PriorityQueue(IEnumerable<KeyValuePair<TPriority, TValue>> data, IComparer<TPriority> comparer)
            {
                if (data == null || comparer == null)
                    throw new ArgumentNullException();

                _comparer = comparer;
                _baseHeap = new List<KeyValuePair<TPriority, TValue>>(data);
                // heapify data
                for (int pos = _baseHeap.Count / 2 - 1; pos >= 0; pos--)
                    HeapifyFromBeginningToEnd(pos);
            }

            #endregion

            #region Merging

            /// <summary>
            /// Merges two priority queues
            /// </summary>
            /// <param name="pq1">first priority queue</param>
            /// <param name="pq2">second priority queue</param>
            /// <returns>resultant priority queue</returns>
            /// <remarks>
            /// source priority queues must have equal comparers,
            /// otherwise <see cref="InvalidOperationException"/> will be thrown
            /// </remarks>
            public static PriorityQueue<TPriority, TValue> MergeQueues(PriorityQueue<TPriority, TValue> pq1, PriorityQueue<TPriority, TValue> pq2)
            {
                if (pq1 == null || pq2 == null)
                    throw new ArgumentNullException();
                if (pq1._comparer != pq2._comparer)
                    throw new InvalidOperationException("Priority queues to be merged must have equal comparers");
                return MergeQueues(pq1, pq2, pq1._comparer);
            }

            /// <summary>
            /// Merges two priority queues and sets specified comparer for resultant priority queue
            /// </summary>
            /// <param name="pq1">first priority queue</param>
            /// <param name="pq2">second priority queue</param>
            /// <param name="comparer">comparer for resultant priority queue</param>
            /// <returns>resultant priority queue</returns>
            public static PriorityQueue<TPriority, TValue> MergeQueues(PriorityQueue<TPriority, TValue> pq1, PriorityQueue<TPriority, TValue> pq2, IComparer<TPriority> comparer)
            {
                if (pq1 == null || pq2 == null || comparer == null)
                    throw new ArgumentNullException();
                // merge data
                PriorityQueue<TPriority, TValue> result = new PriorityQueue<TPriority, TValue>(pq1.Count + pq2.Count, pq1._comparer);
                result._baseHeap.AddRange(pq1._baseHeap);
                result._baseHeap.AddRange(pq2._baseHeap);
                // heapify data
                for (int pos = result._baseHeap.Count / 2 - 1; pos >= 0; pos--)
                    result.HeapifyFromBeginningToEnd(pos);

                return result;
            }

            #endregion

            #region Priority queue operations

            /// <summary>
            /// Enqueues element into priority queue
            /// </summary>
            /// <param name="priority">element priority</param>
            /// <param name="value">element value</param>
            public void Enqueue(TPriority priority, TValue value)
            {
                Insert(priority, value);
            }

            /// <summary>
            /// Dequeues element with minimum priority and return its priority and value as <see cref="KeyValuePair{TPriority,TValue}"/> 
            /// </summary>
            /// <returns>priority and value of the dequeued element</returns>
            /// <remarks>
            /// Method throws <see cref="InvalidOperationException"/> if priority queue is empty
            /// </remarks>
            public KeyValuePair<TPriority, TValue> Dequeue()
            {
                if (!IsEmpty)
                {
                    KeyValuePair<TPriority, TValue> result = _baseHeap[0];
                    DeleteRoot();
                    return result;
                }
                else
                    throw new InvalidOperationException("Priority queue is empty");
            }

            /// <summary>
            /// Dequeues element with minimum priority and return its value
            /// </summary>
            /// <returns>value of the dequeued element</returns>
            /// <remarks>
            /// Method throws <see cref="InvalidOperationException"/> if priority queue is empty
            /// </remarks>
            public TValue DequeueValue()
            {
                return Dequeue().Value;
            }

            /// <summary>
            /// Returns priority and value of the element with minimun priority, without removing it from the queue
            /// </summary>
            /// <returns>priority and value of the element with minimum priority</returns>
            /// <remarks>
            /// Method throws <see cref="InvalidOperationException"/> if priority queue is empty
            /// </remarks>
            public KeyValuePair<TPriority, TValue> Peek()
            {
                if (!IsEmpty)
                    return _baseHeap[0];
                else
                    throw new InvalidOperationException("Priority queue is empty");
            }

            /// <summary>
            /// Returns value of the element with minimun priority, without removing it from the queue
            /// </summary>
            /// <returns>value of the element with minimum priority</returns>
            /// <remarks>
            /// Method throws <see cref="InvalidOperationException"/> if priority queue is empty
            /// </remarks>
            public TValue PeekValue()
            {
                return Peek().Value;
            }

            /// <summary>
            /// Gets whether priority queue is empty
            /// </summary>
            public bool IsEmpty
            {
                get { return _baseHeap.Count == 0; }
            }

            #endregion

            #region Heap operations

            private void ExchangeElements(int pos1, int pos2)
            {
                KeyValuePair<TPriority, TValue> val = _baseHeap[pos1];
                _baseHeap[pos1] = _baseHeap[pos2];
                _baseHeap[pos2] = val;
            }

            private void Insert(TPriority priority, TValue value)
            {
                KeyValuePair<TPriority, TValue> val = new KeyValuePair<TPriority, TValue>(priority, value);
                _baseHeap.Add(val);

                // heap[i] have children heap[2*i + 1] and heap[2*i + 2] and parent heap[(i-1)/ 2];

                // heapify after insert, from end to beginning
                HeapifyFromEndToBeginning(_baseHeap.Count - 1);
            }


            private int HeapifyFromEndToBeginning(int pos)
            {
                if (pos >= _baseHeap.Count) return -1;

                while (pos > 0)
                {
                    int parentPos = (pos - 1) / 2;
                    if (_comparer.Compare(_baseHeap[parentPos].Key, _baseHeap[pos].Key) > 0)
                    {
                        ExchangeElements(parentPos, pos);
                        pos = parentPos;
                    }
                    else break;
                }
                return pos;
            }


            private void DeleteRoot()
            {
                if (_baseHeap.Count <= 1)
                {
                    _baseHeap.Clear();
                    return;
                }

                _baseHeap[0] = _baseHeap[_baseHeap.Count - 1];
                _baseHeap.RemoveAt(_baseHeap.Count - 1);

                // heapify
                HeapifyFromBeginningToEnd(0);
            }

            private void HeapifyFromBeginningToEnd(int pos)
            {
                if (pos >= _baseHeap.Count) return;

                // heap[i] have children heap[2*i + 1] and heap[2*i + 2] and parent heap[(i-1)/ 2];

                while (true)
                {
                    // on each iteration exchange element with its smallest child
                    int smallest = pos;
                    int left = 2 * pos + 1;
                    int right = 2 * pos + 2;
                    if (left < _baseHeap.Count && _comparer.Compare(_baseHeap[smallest].Key, _baseHeap[left].Key) > 0)
                        smallest = left;
                    if (right < _baseHeap.Count && _comparer.Compare(_baseHeap[smallest].Key, _baseHeap[right].Key) > 0)
                        smallest = right;

                    if (smallest != pos)
                    {
                        ExchangeElements(smallest, pos);
                        pos = smallest;
                    }
                    else break;
                }
            }

            #endregion

            #region ICollection<KeyValuePair<TPriority, TValue>> implementation

            /// <summary>
            /// Enqueus element into priority queue
            /// </summary>
            /// <param name="item">element to add</param>
            public void Add(KeyValuePair<TPriority, TValue> item)
            {
                Enqueue(item.Key, item.Value);
            }

            /// <summary>
            /// Clears the collection
            /// </summary>
            public void Clear()
            {
                _baseHeap.Clear();
            }

            /// <summary>
            /// Determines whether the priority queue contains a specific element
            /// </summary>
            /// <param name="item">The object to locate in the priority queue</param>
            /// <returns><c>true</c> if item is found in the priority queue; otherwise, <c>false.</c> </returns>
            public bool Contains(KeyValuePair<TPriority, TValue> item)
            {
                return _baseHeap.Contains(item);
            }

            /// <summary>
            /// Gets number of elements in the priority queue
            /// </summary>
            public int Count
            {
                get { return _baseHeap.Count; }
            }

            /// <summary>
            /// Copies the elements of the priority queue to an Array, starting at a particular Array index. 
            /// </summary>
            /// <param name="array">The one-dimensional Array that is the destination of the elements copied from the priority queue. The Array must have zero-based indexing. </param>
            /// <param name="arrayIndex">The zero-based index in array at which copying begins.</param>
            /// <remarks>
            /// It is not guaranteed that items will be copied in the sorted order.
            /// </remarks>
            public void CopyTo(KeyValuePair<TPriority, TValue>[] array, int arrayIndex)
            {
                _baseHeap.CopyTo(array, arrayIndex);
            }

            /// <summary>
            /// Gets a value indicating whether the collection is read-only. 
            /// </summary>
            /// <remarks>
            /// For priority queue this property returns <c>false</c>.
            /// </remarks>
            public bool IsReadOnly
            {
                get { return false; }
            }

            /// <summary>
            /// Removes the first occurrence of a specific object from the priority queue. 
            /// </summary>
            /// <param name="item">The object to remove from the ICollection <(Of <(T >)>). </param>
            /// <returns><c>true</c> if item was successfully removed from the priority queue.
            /// This method returns false if item is not found in the collection. </returns>
            public bool Remove(KeyValuePair<TPriority, TValue> item)
            {
                // find element in the collection and remove it
                int elementIdx = _baseHeap.IndexOf(item);
                if (elementIdx < 0) return false;

                //remove element
                _baseHeap[elementIdx] = _baseHeap[_baseHeap.Count - 1];
                _baseHeap.RemoveAt(_baseHeap.Count - 1);

                // heapify
                int newPos = HeapifyFromEndToBeginning(elementIdx);
                if (newPos == elementIdx)
                    HeapifyFromBeginningToEnd(elementIdx);

                return true;
            }

            /// <summary>
            /// Returns an enumerator that iterates through the collection.
            /// </summary>
            /// <returns>Enumerator</returns>
            /// <remarks>
            /// Returned enumerator does not iterate elements in sorted order.</remarks>
            public IEnumerator<KeyValuePair<TPriority, TValue>> GetEnumerator()
            {
                return _baseHeap.GetEnumerator();
            }

            /// <summary>
            /// Returns an enumerator that iterates through the collection.
            /// </summary>
            /// <returns>Enumerator</returns>
            /// <remarks>
            /// Returned enumerator does not iterate elements in sorted order.</remarks>
            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }

            #endregion
        }
    }

