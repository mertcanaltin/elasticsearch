/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.collapse;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.grouping.CollapsingTopDocsCollector;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.mapper.MappedFieldType.CollapseType;
import org.elasticsearch.search.sort.SortAndFormats;

import java.util.List;

/**
 * Context used for field collapsing
 */
public class CollapseContext {
    private final String fieldName;
    private final MappedFieldType fieldType;
    private final List<InnerHitBuilder> innerHits;
    private final SortAndFormats sort;

    public CollapseContext(String fieldName,
                           MappedFieldType fieldType,
                           List<InnerHitBuilder> innerHits,
                           SortAndFormats sort) {
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.innerHits = innerHits;
        this.sort = sort;
    }

    /**
     * The requested field name to collapse on.
     */
    public String getFieldName() {
        return fieldName;
    }

    /**
     * The field type used for collapsing
     **/
    public MappedFieldType getFieldType() {
        return fieldType;
    }

    /**
     * The inner hit options to expand the collapsed results
     **/
    public List<InnerHitBuilder> getInnerHit() {
        return innerHits;
    }

    /**
     * The sort options for selecting top docs per group
     **/
    public SortAndFormats getSort() {
        return sort;
    }

    public CollapsingTopDocsCollector<?> createTopDocs(IndexSearcher indexSearcher, Query query, Sort sort, int topN) {
        Sort collapseSort = this.sort == null ? null : this.sort.sort;

        if (fieldType.collapseType() == CollapseType.KEYWORD) {
            return CollapsingTopDocsCollector.createKeyword(indexSearcher, query, fieldName, fieldType, sort, collapseSort, topN);
        } else if (fieldType.collapseType() == CollapseType.NUMERIC) {
            return CollapsingTopDocsCollector.createNumeric(indexSearcher, query, fieldName, fieldType, sort, collapseSort, topN);
        } else {
            throw new IllegalStateException("collapse is not supported on this field type");
        }
    }
}
