/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.search.grouping;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.*;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.Collection;
import java.util.ArrayList;

public final class SortingCollectedSearchGroups<T> extends SecondPassGroupingCollector<T> {
    private final FieldComparator<?>[] comparators;
    private final LeafFieldComparator[] leafComparators;
    private final int[] reversed;
    private final int compIDXEnd;
    public final TreeSet<CollectedSearchGroup<T>> orderedGroups;
    private int comparatorSlot = 0;
    private int docBase;

    public SortingCollectedSearchGroups(GroupSelector<T> groupSelector, Collection<SearchGroup<T>> searchGroups, Sort sort) {
        super(groupSelector, searchGroups, new ValuesReducer<>(sort));

        final SortField[] sortFields = sort.getSort();

        comparators = new FieldComparator[sortFields.length];
        leafComparators = new LeafFieldComparator[sortFields.length];

        compIDXEnd = comparators.length - 1;

        reversed = new int[sortFields.length];
        for (int i = 0; i < sortFields.length; i++) {
            final SortField sortField = sortFields[i];

            comparators[i] = sortField.getComparator(searchGroups.size() + 1, i);
            reversed[i] = sortField.getReverse() ? -1 : 1;
        }

        final Comparator<CollectedSearchGroup<?>> comparator = (o1, o2) -> {
            for (int compIDX = 0; ; compIDX++) {

                FieldComparator<?> fc = comparators[compIDX];

                final int c = reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);

                if (c != 0) {
                    return c;
                } else if (compIDX == compIDXEnd) {
                    return o1.topDoc - o2.topDoc;
                }
            }
        };

        orderedGroups = new TreeSet<>(comparator);
    }

    private static class ValuesCollector extends SimpleCollector {
        private final boolean needsScores;

        private ValuesCollector(boolean needsScores) {
            this.needsScores = needsScores;
        }

        @Override
        public void collect(int doc) throws IOException {

        }

        @Override
        public ScoreMode scoreMode() {
            return needsScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private static class ValuesReducer<T> extends GroupReducer<T, ValuesCollector> {
        private final Sort sort;

        private ValuesReducer(Sort sort) {
            this.sort = sort;
        }

        @Override
        public boolean needsScores() {
            return sort.needsScores();
        }

        @Override
        protected ValuesCollector newCollector() {
            return new ValuesCollector(needsScores());
        }
    }

    public Collection<SearchGroup<T>> getTopGroups() throws IOException {
        assert orderedGroups.size() > 0;

        for (LeafFieldComparator fc : leafComparators) {
            fc.setBottom(orderedGroups.last().comparatorSlot);
        }

        final Collection<SearchGroup<T>> result = new ArrayList<>();

        final int sortFieldCount = comparators.length;

        for (CollectedSearchGroup<T> group : orderedGroups) {
            SearchGroup<T> searchGroup = new SearchGroup<>();

            searchGroup.groupValue = group.groupValue;
            searchGroup.sortValues = new Object[sortFieldCount];

            for (int sortFieldIDX = 0; sortFieldIDX < sortFieldCount; sortFieldIDX++) {
                searchGroup.sortValues[sortFieldIDX] = comparators[sortFieldIDX].value(group.comparatorSlot);
            }

            result.add(searchGroup);
        }

        return result;
    }

    @Override
    public void collect(int doc) throws IOException {
        totalHitCount++;

        if (groupSelector.advanceTo(doc) == GroupSelector.State.SKIP)
            return;

        totalGroupedHitCount++;

        CollectedSearchGroup<T> sg = new CollectedSearchGroup<>();

        sg.groupValue = groupSelector.copyValue();
        sg.comparatorSlot = comparatorSlot++;
        sg.topDoc = docBase + doc;

        for (LeafFieldComparator fc : leafComparators) {
            fc.copy(sg.comparatorSlot, doc);
        }

        orderedGroups.add(sg);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
        docBase = readerContext.docBase;

        for (int i = 0; i < comparators.length; i++) {
            leafComparators[i] = comparators[i].getLeafComparator(readerContext);
        }

        super.doSetNextReader(readerContext);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        groupSelector.setScorer(scorer);

        for (LeafFieldComparator comparator : leafComparators) {
            comparator.setScorer(scorer);
        }
    }
}
