/*
 * Copyright 2007-2107 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.ymate.platform.module.search.support;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import net.ymate.platform.commons.lang.BlurObject;
import net.ymate.platform.commons.lang.PairObject;
import net.ymate.platform.commons.util.ClassUtils;
import net.ymate.platform.commons.util.ClassUtils.ClassBeanWrapper;
import net.ymate.platform.module.search.ISearchable;
import net.ymate.platform.module.search.Searchs;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;

/**
 * <p>
 * SearchHelper
 * </p>
 * <p>
 * 搜索构建助手类；
 * </p>
 * 
 * @author 刘镇(suninformation@163.com)
 * @version 0.0.0
 *          <table style="border:1px solid gray;">
 *          <tr>
 *          <th width="100px">版本号</th><th width="100px">动作</th><th
 *          width="100px">修改人</th><th width="100px">修改时间</th>
 *          </tr>
 *          <!-- 以 Table 方式书写修改历史 -->
 *          <tr>
 *          <td>0.0.0</td>
 *          <td>创建类</td>
 *          <td>刘镇</td>
 *          <td>2014年3月5日下午11:36:41</td>
 *          </tr>
 *          </table>
 */
public class SearchHelper<T extends ISearchable> {

	private static final Log _LOG = LogFactory.getLog(SearchHelper.class);

	private Class<T> __class;
	private IndexSearcher __searcher;
	private IndexReader __reader;

	private Query __query;
	private Sort __sort;
	private Filter __filter;

	private int __page = 1;
	private int __pageSize = 20;
	private int __maxPage = 100;

	/**
	 * 构造器
	 * 
	 * @param searchableClass
	 */
	public SearchHelper(Class<T> searchableClass) {
		__class = searchableClass;
		__searcher = Searchs.getIndexSearcher(__class);
		__reader = __searcher.getIndexReader();
		__reader.incRef();
	}

	public void release() {
		try {
			__reader.decRef();
		} catch(IOException ex) {
			_LOG.error("Something is wrong when decrease the reference of IndexReader", ex);
		}
	}

	public SearchHelper<T> query(Query query) {
		this.__query = query;
		return this;
	}

	public SearchHelper<T> query(QueryParser parser, String value) {
		try {
			return query(parser.parse(value));
		} catch (ParseException e) {
			_LOG.error("Can not parse the value " + value , e);
		}
		return this;
	}

	public SearchHelper<T> queryTerm(String field, String value) {
		return query(new TermQuery(new Term(field, value)));
	}

	public SearchHelper<T> query(String field, String value) {
		return query(field,value, Operator.OR);
	}

	public SearchHelper<T> query(String field, String value, Operator opt) {
		QueryParser _parser = new QueryParser(Searchs.getConfig().getLuceneVersion(), field, Searchs.getConfig().getAnalyzerImpl());
		_parser.setDefaultOperator(opt);
		return query(_parser, value);
	}

	public SearchHelper<T> query(String[] fields, String value) {
		return query(fields, value, Operator.OR);
	}

	public SearchHelper<T> query(String[] fields, String value, Operator opt) {
		QueryParser _parser = new MultiFieldQueryParser(Searchs.getConfig().getLuceneVersion(), fields, Searchs.getConfig().getAnalyzerImpl());
        _parser.setDefaultOperator(opt);
        return query(_parser, value);
	}

	public SearchHelper<T> query(String[] fields, Occur[] occurs, String value) {
		Query _query = null;
		try {
			_query = MultiFieldQueryParser.parse(Searchs.getConfig().getLuceneVersion(), value, fields, occurs, Searchs.getConfig().getAnalyzerImpl());
		} catch (ParseException e) {
			_LOG.error("MultiFieldQueryParser can not parse the value " + value , e);
		}
        return query(_query);
	}

	public SearchHelper<T> query(String field, int value) {
		return query(NumericRangeQuery.newIntRange(field, value, value, true, true));
	}

	public SearchHelper<T> query(String field, int minValue, int maxValue) {
		return query(NumericRangeQuery.newIntRange(field, minValue, maxValue, true, true));
	}

	public SearchHelper<T> query(String field, long value) {
		return query(NumericRangeQuery.newLongRange(field, value, value, true, true));
	}

	public SearchHelper<T> query(String field, long minValue, long maxValue) {
		return query(NumericRangeQuery.newLongRange(field, minValue, maxValue, true, true));
	}

	public SearchHelper<T> query(String field, float value) {
		return query(NumericRangeQuery.newFloatRange(field, value, value, true, true));
	}

	public SearchHelper<T> query(String field, float minValue, float maxValue) {
		return query(NumericRangeQuery.newFloatRange(field, minValue, maxValue, true, true));
	}

	public SearchHelper<T> query(String field, double value) {
		return query(NumericRangeQuery.newDoubleRange(field, value, value, true, true));
	}

	public SearchHelper<T> query(String field, double minValue, double maxValue) {
		return query(NumericRangeQuery.newDoubleRange(field, minValue, maxValue, true, true));
	}

	public SearchHelper<T> query(String field, Date begin, Date end) {
		long beginTime = begin.getTime();
		long endTime = end.getTime();
		return query(field, beginTime, endTime);
	}

	public SearchHelper<T> query(String field, Timestamp begin, Timestamp end) {
		long beginTime = begin.getTime();
		long endTime = end.getTime();
		return query(field, beginTime, endTime);
	}

	public SearchHelper<T> query(String field, boolean value) {
		if (value) {
			return query(field, "true");
		} else {
			return query(field, "false");
		}
	}

	public SearchHelper<T> query(String field, char value) {
		return query(field, String.valueOf(value));
	}

	public SearchHelper<T> sort(Sort sort) {
		this.__sort = sort;
		return this;
	}

	public SearchHelper<T> filter(Filter filter) {
		this.__filter = filter;
		return this;
	}

	public SearchHelper<T> page(int page) {
		this.__page = page;
		return this;
	}

	public SearchHelper<T> pageSize(int pageSize) {
		this.__pageSize = pageSize;
		return this;
	}

	public SearchHelper<T> maxPage(int maxPage) {
		this.__maxPage = maxPage;
		return this;
	}

	public PairObject<Integer, List<T>> execute() {
		TopDocs _topDocs = null;
		List<T> _results = null;
		int _resultCount = 0;
		try {
			if (__sort == null) {
				_topDocs = __searcher.search(__query, __filter, (__page > __maxPage ? __maxPage : __page) * __pageSize);
			} else {
				_topDocs = __searcher.search(__query, __filter, (__page > __maxPage ? __maxPage : __page) * __pageSize, __sort);
			}
		} catch (IOException e) {
			_LOG.error("Something is wrong when doing lucene search", e);
		}
		if (_topDocs == null || _topDocs.totalHits == 0) {
			_results = Collections.emptyList();
		} else {
			_resultCount = _topDocs.totalHits;
			ScoreDoc[] _docs = _topDocs.scoreDocs;
			//
			int _beginIdx = (__page - 1) * __pageSize;
			int _endIdx = _beginIdx + __pageSize;
			if (_endIdx > _resultCount) {
				_endIdx = _resultCount;
			}
			_results = new ArrayList<T>();
			for (int _idx = _beginIdx; _idx < _endIdx; _idx++) {
				try {
					Document _document = __searcher.doc(_docs[_idx].doc);
					T _target = __doParserIndexDocument(_document);
					if (StringUtils.isNotBlank(_target.getId())) {
						_results.add(_target);
					}
				} catch (IOException e) {
					_LOG.error("Lucene IndexSearcher can not get the document", e);
				}
			}
		}
		return new PairObject<Integer, List<T>>(_resultCount, _results);
	}

	private T __doParserIndexDocument(Document document) {
		IndexedMeta _meta = Searchs.getIndexedMeta(__class);
		ClassBeanWrapper<T> _wrapper = ClassUtils.wrapper(__class);
		for (String _fieldName : _meta.getFieldNames()) {
			if (_wrapper.getFieldNames().contains(_fieldName)) {
				_wrapper.setValue(_fieldName, new BlurObject(document.get(_fieldName)).toObjectValue(_wrapper.getFieldType(_fieldName)));
			}
		}
		return _wrapper.getTarget();
	}
  
}
