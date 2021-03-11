/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.search.grouping;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import static org.apache.lucene.search.SortField.Type.SCORE;

/**
 * A collector that groups documents based on field values and returns {@link CollapseTopFieldDocs}
 * output. The collapsing is done in a single pass by selecting only the top sorted document per collapse key.
 * The value used for the collapse key of each group can be found in {@link CollapseTopFieldDocs#collapseValues}.
 *
 * TODO: If the sort is based on score we should propagate the mininum competitive score when <code>orderedGroups</code> is full.
 * This is safe for collapsing since the group <code>sort</code> is the same as the query sort.
 */
public final class CollapsingTopDocsCollector<T> extends FirstPassGroupingCollector<T> {
    protected final String collapseField;

    protected final Sort sort;
    @Nullable protected final Sort collapseSort;
    protected Scorable scorer;

    private int totalHitCount;
    protected final IndexSearcher indexSearcher;
    protected final Query query;

    CollapsingTopDocsCollector(IndexSearcher indexSearcher, Query query, GroupSelector<T> groupSelector, String collapseField, Sort sort, @Nullable Sort collapseSort, int topN) {
        super(groupSelector, collapseSort == null ? sort : collapseSort, topN);
        this.indexSearcher = indexSearcher;
        this.query = query;
        this.collapseField = collapseField;
        this.sort = sort;
        this.collapseSort = collapseSort;
    }

    /**
     * Transform {@link FirstPassGroupingCollector#getTopGroups(int)} output in
     * {@link CollapseTopFieldDocs}. The collapsing needs only one pass so we can get the final top docs at the end
     * of the first pass.
     */
    public CollapseTopFieldDocs getTopDocs() throws IOException {
        Collection<SearchGroup<T>> groups = super.getTopGroups(0);
        TreeSet<CollectedSearchGroup<T>> orderedGroups = super.orderedGroups;

        if (collapseSort != null) {
            SortingCollectedSearchGroups<T> sortingCollectedSearchGroups = new SortingCollectedSearchGroups<T>(
                getGroupSelector(),
                new ArrayList<>(orderedGroups),
                sort
            );
            indexSearcher.search(query, sortingCollectedSearchGroups);
            groups = sortingCollectedSearchGroups.getTopGroups();
            orderedGroups = sortingCollectedSearchGroups.orderedGroups;
        }

        if (groups == null) {
            TotalHits totalHits = new TotalHits(0, TotalHits.Relation.EQUAL_TO);
            return new CollapseTopFieldDocs(collapseField, totalHits, new ScoreDoc[0], sort.getSort(), new Object[0]);
        }
        FieldDoc[] docs = new FieldDoc[groups.size()];
        Object[] collapseValues = new Object[groups.size()];
        int scorePos = -1;
        for (int index = 0; index < sort.getSort().length; index++) {
            SortField sortField = sort.getSort()[index];
            if (sortField.getType() == SCORE) {
                scorePos = index;
                break;
            }
        }
        int pos = 0;
        Iterator<CollectedSearchGroup<T>> it = orderedGroups.iterator();
        for (SearchGroup<T> group : groups) {
            assert it.hasNext();
            CollectedSearchGroup<T> col = it.next();
            float score = Float.NaN;
            if (scorePos != -1) {
                score = (float) group.sortValues[scorePos];
            }
            docs[pos] = new FieldDoc(col.topDoc, score, group.sortValues);
            collapseValues[pos] = group.groupValue;
            pos++;
        }
        TotalHits totalHits = new TotalHits(totalHitCount, TotalHits.Relation.EQUAL_TO);
        return new CollapseTopFieldDocs(collapseField, totalHits, docs, sort.getSort(), collapseValues);
    }

    @Override
    public ScoreMode scoreMode() {
        if (super.scoreMode().needsScores()) {
            return ScoreMode.COMPLETE;
        } else {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        super.setScorer(scorer);
        this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
        super.collect(doc);
        totalHitCount++;
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.NumericDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedNumericDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param indexSearcher     The index searcher
     * @param query             The query
     * @param collapseField     The sort field used to group documents.
     * @param collapseFieldType The {@link MappedFieldType} for this sort field.
     * @param sort              The {@link Sort} used to sort the collapsed hits.
     * @param collapseSort      {@link Sort}
     *                          The collapsing keeps only the top sorted document per collapsed key.
     *                          This must be non-null, ie, if you want to groupSort by relevance
     *                          use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     */
    public static CollapsingTopDocsCollector<?> createNumeric(IndexSearcher indexSearcher,
                                                              Query query,
                                                              String collapseField,
                                                              MappedFieldType collapseFieldType,
                                                              Sort sort,
                                                              @Nullable Sort collapseSort,
                                                              int topN)  {
        return new CollapsingTopDocsCollector<>(indexSearcher, query, new CollapsingDocValuesSource.Numeric(collapseFieldType),
                collapseField, sort, collapseSort, topN);
    }

    /**
     * Create a collapsing top docs collector on a {@link org.apache.lucene.index.SortedDocValues} field.
     * It accepts also {@link org.apache.lucene.index.SortedSetDocValues} field but
     * the collect will fail with an {@link IllegalStateException} if a document contains more than one value for the
     * field.
     *
     * @param indexSearcher     The index searcher
     * @param query             The query
     * @param collapseField     The sort field used to group documents.
     * @param collapseFieldType The {@link MappedFieldType} for this sort field.
     * @param sort              The {@link Sort} used to sort the collapsed hits. The collapsing keeps only the top sorted
     *                          document per collapsed key.
     * @param collapseSort      {@link Sort}
     *                          This must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
     * @param topN              How many top groups to keep.
     */
    public static CollapsingTopDocsCollector<?> createKeyword(IndexSearcher indexSearcher,
                                                              Query query,
                                                              String collapseField,
                                                              MappedFieldType collapseFieldType,
                                                              Sort sort,
                                                              @Nullable Sort collapseSort,
                                                              int topN)  {
        return new CollapsingTopDocsCollector<>(indexSearcher, query, new CollapsingDocValuesSource.Keyword(collapseFieldType),
                collapseField, sort, collapseSort, topN);
    }
}
