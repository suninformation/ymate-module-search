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
package net.ymate.platform.module.search;

import java.sql.Timestamp;
import java.util.Date;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParser.Operator;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

/**
 * <p>
 * SearchParser
 * </p>
 * <p>
 * 查询分析器；
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
 *          <td>2014年3月5日下午11:41:25</td>
 *          </tr>
 *          </table>
 */
public class SearchParser {

	/**
     * Create a term query. 
     * <p>Note: in fact, it does not parse. It just create a term query.</p>
     * @param field
     * @param value
     * @return 
     */
    public static Query parseTerm(String field, String value){
        Term t = new Term(field, value);
        return new TermQuery(t);
    }
    
    public static Query parse(String field, String value){
        return parse(field, value, Operator.OR);
    }
    
    public static Query parse(String field, String value, Operator op){
        QueryParser parser = new QueryParser(Searchs.getConfig().getLuceneVersion(), field, Searchs.getConfig().getAnalyzerImpl());
        parser.setDefaultOperator(op);
        return parse(parser, value);
    }
    
    public static Query parse(String[] fields, String value){
        return parse(fields, value, Operator.OR);
    }
    
    public static Query parse(String[] fields, String value, Operator op){
        QueryParser parser = new MultiFieldQueryParser(Searchs.getConfig().getLuceneVersion(), fields, Searchs.getConfig().getAnalyzerImpl());
        parser.setDefaultOperator(op);
        return parse(parser, value);
    }
    
    public static Query parse(String[] fields, Occur[] occurs, String value){
        Query query = null;
        try{
            query = MultiFieldQueryParser.parse(Searchs.getConfig().getLuceneVersion(), value, fields, occurs, Searchs.getConfig().getAnalyzerImpl());
        }catch(ParseException ex){
            //logger.error("MultiFieldQueryParser can not parse the value " + value , ex);
        }
        return query;
    }
    
    private static Query parse(QueryParser parser, String value){
        Query query = null;
        try{
            query = parser.parse(value);
        }catch(ParseException ex){
            // logger.error("Can not parse the value " + value , ex);
        }
        return query;
    }
    
    public static Query parse(String field, int value){
        return NumericRangeQuery.newIntRange(field, value, value, true, true);
    }
    
    public static Query parse(String field, int minValue, int maxValue){
        return NumericRangeQuery.newIntRange(field, minValue, maxValue, true, true);
    }
    
    public static Query parse(String field, long value){
        return NumericRangeQuery.newLongRange(field, value, value, true, true);
    }
    
    public static Query parse(String field, long minValue, long maxValue){
        return NumericRangeQuery.newLongRange(field, minValue, maxValue, true, true);
    }
    
    public static Query parse(String field, float value){
        return NumericRangeQuery.newFloatRange(field, value, value, true, true);
    }
    
    public static Query parse(String field, float minValue, float maxValue){
        return NumericRangeQuery.newFloatRange(field, minValue, maxValue, true, true);
    }
    
    public static Query parse(String field, double value){
        return NumericRangeQuery.newDoubleRange(field, value, value, true, true);
    }
    
    public static Query parse(String field, double minValue, double maxValue){
        return NumericRangeQuery.newDoubleRange(field, minValue, maxValue, true, true);
    }
    
    /**
     * Date value is converted to long value, same as it's field type in index file. 
     * @param field
     * @param begin
     * @param end
     * @return 
     */
    public static Query parse(String field, Date begin, Date end){
        long beginTime = begin.getTime();
        long endTime = end.getTime();
        return parse(field, beginTime, endTime);
    }
    
    /**
     * Timestamp value is converted to long value, same as it's field type in index file. 
     * @param field
     * @param begin
     * @param end
     * @return 
     */
    public static Query parse(String field, Timestamp begin, Timestamp end){
        long beginTime = begin.getTime();
        long endTime = end.getTime();
        return parse(field, beginTime, endTime);
    }
    
    /**
     * Boolean value is converted to string value, same as it's field type in index file. 
     * @param field
     * @param value
     * @return 
     */
    public static Query parse(String field, boolean value){
        if(value){
            return parse(field, "true");
        }else{
            return parse(field, "false");
        }
    }
    
    /**
     * Char value is converted to string value, same as it's field type in index file. 
     * @param field
     * @param value
     * @return 
     */
    public static Query parse(String field, char value){
        return parse(field, String.valueOf(value));
    }

}
