package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        List<Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            recordList.add(records.next());
        }
        recordList.sort(comparator);
        return makeRun(recordList);
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    // 我们从算法设计的角度来分析一下mergeSort的写法，这次我们来写一点不一样的mergeSort。我们不再使用递归的方式，而是使用queue的方式来实现mergeSort。
    private Run mergeTwoRuns(Run run1, Run run2) {
        List<Record> mergedRecords = new ArrayList<>();
        Iterator<Record> iter1 = run1.iterator();
        Iterator<Record> iter2 = run2.iterator();

        Record record1 = iter1.hasNext() ? iter1.next() : null;
        Record record2 = iter2.hasNext() ? iter2.next() : null;

        while (record1 != null && record2 != null) {
            if (comparator.compare(record1, record2) < 0) {
                mergedRecords.add(record1);
                record1 = iter1.hasNext() ? iter1.next() : null;
            } else {
                mergedRecords.add(record2);
                record2 = iter2.hasNext() ? iter2.next() : null;
            }
        }

        while (record1 != null) {
            mergedRecords.add(record1);
            record1 = iter1.hasNext() ? iter1.next() : null;
        }

        while (record2 != null) {
            mergedRecords.add(record2);
            record2 = iter2.hasNext() ? iter2.next() : null;
        }
        return makeRun(mergedRecords);
    }

    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);
        // TODO(proj3_part1): implement
        List<Record> mergedRecords = new ArrayList<>();
        Queue<Run> queue = new LinkedList<>(runs);
        while (queue.size() > 1) {
            Run run1 = queue.poll();
            Run run2 = queue.poll();
            queue.add(mergeTwoRuns(run1, run2));
        }
        return queue.isEmpty() ? null : queue.poll();
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        List<Run> result = new ArrayList<>();
        int numRuns = runs.size();
        int numBuffers = this.numBuffers - 1;
        for (int i = 0; i < numRuns; i += numBuffers) {
            int end = Math.min(i + numBuffers, numRuns);
            List<Run> sublist = runs.subList(i, end);
            result.add(mergeSortedRuns(sublist));
        }
        return result;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        List<Run> initialRuns = new ArrayList<>();

        while (sourceIterator.hasNext()) {
            Iterator<Record> blockIterator = getBlockIterator(sourceIterator, getSchema(), numBuffers);
            initialRuns.add(sortRun(blockIterator));
        }

        List<Run> finalRuns = initialRuns;
        while (finalRuns.size() > 1) {
            finalRuns = mergePass(finalRuns);
        }

        // TODO(proj3_part1): implement
        return finalRuns.get(0); // TODO(proj3_part1): replace this!
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}

