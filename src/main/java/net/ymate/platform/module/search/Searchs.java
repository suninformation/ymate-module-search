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

import net.ymate.platform.commons.lang.BlurObject;
import net.ymate.platform.commons.util.ClassUtils;
import net.ymate.platform.commons.util.ClassUtils.ClassBeanWrapper;
import net.ymate.platform.commons.util.RuntimeUtils;
import net.ymate.platform.module.search.annotation.IndexField;
import net.ymate.platform.module.search.handler.ICallbackHandler;
import net.ymate.platform.module.search.handler.IRebuildHandler;
import net.ymate.platform.module.search.support.IndexHelper;
import net.ymate.platform.module.search.support.IndexedMeta;
import net.ymate.platform.module.search.support.SearchHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


/**
 * <p>
 * Searchs
 * </p>
 * <p>
 * 全文检索框架管理器;
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
 *          <td>2014年3月5日下午8:35:38</td>
 *          </tr>
 *          </table>
 */
public class Searchs {

    private static final Log _LOG = LogFactory.getLog(Searchs.class);

    /**
     * 当前全文检索框架初始化配置对象
     */
    private static ISearchConfig __CFG_CONFIG;

    private static boolean __IS_INITED;

    private static IndexHelper __indexHelper;

    /**
     * 索引模型元数据描述类缓存
     */
    private static Map<Class<?>, IndexedMeta> __cacheIndexedMetas = new ConcurrentHashMap<Class<?>, IndexedMeta>();

    private static ExecutorService __executor;

    public final static Map<String, IndexSearcher> __SEARCH_CACHES = new ConcurrentHashMap<String, IndexSearcher>();

    public final static Map<String, IndexWriter> __WRITER_CACHES = new ConcurrentHashMap<String, IndexWriter>();


    /**
     * 初始化全文检索框架管理器
     *
     * @param config
     * @throws SearchException
     */
    public static void initialize(ISearchConfig config) throws SearchException {
        if (!__IS_INITED) {
            __CFG_CONFIG = config;
            //
            try {
                __doStart();
            } catch (Throwable e) {
                throw new SearchException(RuntimeUtils.unwrapThrow(e));
            }
            //
            __IS_INITED = true;
        }
    }

    private static void __doStart() {
        int _poolSize = __CFG_CONFIG.getThreadPoolSize();
        if (_poolSize <= 0) {
            _poolSize = Runtime.getRuntime().availableProcessors() * 2 + 1;
        }
        __executor = Executors.newFixedThreadPool(_poolSize);
        //
        __indexHelper = new IndexHelper(__CFG_CONFIG);
    }

    public static void destroy() {
        if (__IS_INITED) {
            __IS_INITED = false;
            //
            __doStopSafed(__executor);
            __indexHelper.release();
        }
    }

    public static void __doStopSafed(ExecutorService pool) {
        if (pool != null) {
            pool.shutdown();
            try {
                if (!pool.awaitTermination(30, TimeUnit.SECONDS)) {
                    pool.shutdownNow();
                }
            } catch (InterruptedException ex) {
                // Nothing..
                ex.printStackTrace();
            }
        }
    }

    private static void __doCheckModuleInited() throws RuntimeException {
        if (!__IS_INITED) {
            throw new RuntimeException("YMP Module Search was not Inited");
        }
    }

    /**
     * @return 获取当前全文检索框架初始化配置对象
     */
    public static ISearchConfig getConfig() {
        __doCheckModuleInited();
        return __CFG_CONFIG;
    }

    /**
     * @return 判断是否已初始化完成
     */
    public static boolean isInited() {
        return __IS_INITED;
    }

    public static <T extends ISearchable> SearchHelper<T> createSearch(Class<T> searchableClass) {
        __doCheckModuleInited();
        return new SearchHelper<T>(searchableClass);
    }

    public static IndexSearcher getIndexSearcher(Class<? extends ISearchable> searchableClass) {
        __doCheckModuleInited();
        return __doGetIndexSearcher(getIndexedMeta(searchableClass).getIndexName());
    }

    private static IndexSearcher __doGetIndexSearcher(String name) {
        IndexSearcher _searcher = __SEARCH_CACHES.get(name);
        if (_searcher != null) {
            return _searcher;
        }
        return __doGetOrCreateIndexSearcher(name);
    }

    private synchronized static IndexSearcher __doGetOrCreateIndexSearcher(String name) {
        IndexSearcher _searcher = __SEARCH_CACHES.get(name);
        if (_searcher == null) {
            IndexWriter _writer = getIndexWriter(name);
            try {
                _searcher = new IndexSearcher(DirectoryReader.open(_writer, true));
                __SEARCH_CACHES.put(name, _searcher);
            } catch (Exception ex) {
                _LOG.error("Open IndexWriter [" + name + "] Error:", ex);
            }
        }
        return _searcher;
    }

    private static IndexWriter getIndexWriter(String name) {
        IndexWriter _writer = __WRITER_CACHES.get(name);
        if (_writer != null) {
            return _writer;
        }
        return __doGetOrCreateIndexWriter(name);
    }

    private synchronized static IndexWriter __doGetOrCreateIndexWriter(String name) {
        IndexWriter _writer = __WRITER_CACHES.get(name);
        if (_writer == null) {
            Version _version = getConfig().getLuceneVersion();
            if (_version == null) {
                _version = Version.LUCENE_46;
            }
            Analyzer _analyzer = getConfig().getAnalyzerImpl();
            if (_analyzer == null) {
                _analyzer = new StandardAnalyzer(_version);
            }
            IndexWriterConfig _conf = new IndexWriterConfig(_version, _analyzer);
            double _bufferSize = getConfig().getBufferSize();
            _conf.setRAMBufferSizeMB(_bufferSize > 0 ? _bufferSize : 16);
            try {
                String _path = getConfig().getDirectoryPath();
                Directory _directory = null;
                if (StringUtils.isBlank(_path)) {
                    _directory = new RAMDirectory();
                } else {
                    _directory = FSDirectory.open(new File(_path + "/" + name));
                }
                _writer = new IndexWriter(_directory, _conf);
            } catch (IOException ex) {
                _LOG.error("Create IndexWriter [" + name + "] Error:", ex);
            }
            __WRITER_CACHES.put(name, _writer);
        }
        return _writer;
    }

    public static void indexCreate(final ISearchable searchable, final ICallbackHandler handler) {
        __doCheckModuleInited();
        __executor.execute(new Runnable() {

            public void run() {
                IndexedMeta _meta = getIndexedMeta(searchable);
                IndexWriter _writer = getIndexWriter(_meta.getIndexName());
                Document _doc = __doIndexDocumentCreate(searchable);
                try {
                    _writer.addDocument(_doc);
                    if (handler != null) {
                        handler.onIndexCreated(searchable);
                    }
                } catch (IOException ex) {
                    _LOG.error("IndexWriter Add Document Error:", ex);
                } finally {
                    try {
                        _writer.commit();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public static void indexCreate(ISearchable searchable) {
        indexCreate(searchable, null);
    }

    public static void indexUpdate(final ISearchable searchable, final ICallbackHandler handler) {
        __doCheckModuleInited();
        __executor.execute(new Runnable() {

            public void run() {
                IndexedMeta _meta = getIndexedMeta(searchable);
                IndexWriter _writer = getIndexWriter(_meta.getIndexName());
                Document _doc = __doIndexDocumentCreate(searchable);
                Term term = new Term(IndexedMeta.FIELD_ID, searchable.getId());
                try {
                    _writer.updateDocument(term, _doc);
                    if (handler != null) {
                        handler.onIndexUpdated(searchable);
                    }
                } catch (IOException ex) {
                    _LOG.error("IndexWriter Update Document Error:", ex);
                }
            }
        });
    }

    public static void indexUpdate(ISearchable searchable) {
        indexUpdate(searchable, null);
    }

    public static void indexRemove(final Class<? extends ISearchable> searchableClass, final String id, final ICallbackHandler handler) {
        __doCheckModuleInited();
        __executor.execute(new Runnable() {

            public void run() {
                IndexedMeta _meta = getIndexedMeta(searchableClass);
                IndexWriter _writer = getIndexWriter(_meta.getIndexName());
                Term term = new Term(IndexedMeta.FIELD_ID, id);
                try {
                    _writer.deleteDocuments(term);
                    if (handler != null) {
                        handler.onIndexRemoved(searchableClass, id);
                    }
                } catch (IOException ex) {
                    _LOG.error("IndexWriter Delete Document Error:", ex);
                }
            }
        });
    }

    public static void indexRemove(Class<? extends ISearchable> searchableClass, String id) {
        indexRemove(searchableClass, id, null);
    }

    public static void indexRebuild(final Class<? extends ISearchable> searchableClass, final int batchSize, final IRebuildHandler handler) {
        __doCheckModuleInited();
        __doIndexRebuild(getIndexedMeta(searchableClass).getIndexName(), batchSize, handler);
    }

    private static void __doIndexRebuild(final String name, final int batchSize, final IRebuildHandler handler) {
        if (__indexHelper.__doCheckBuildWorking(name)) {
            return;
        }
        __executor.execute(new Runnable() {

            public void run() {
                __indexHelper.__doSetBuildWorking(name, true);
                //
                IndexWriter _writer = getIndexWriter(name);
                try {
                    _writer.deleteAll();
                    //
                    int _batchSize = batchSize > 0 ? batchSize : 100;
                    long _count = handler.getAmount();
                    long _batchCount = 0;
                    if (_count % _batchSize > 0) {
                        _batchCount = _count / _batchSize + 1;
                    } else {
                        _batchCount = _count / _batchSize;
                    }
                    //
                    for (int _idx = 1; _idx <= _batchCount; _idx++) {
                        List<? extends ISearchable> _datas = handler.getBatchDatas(_idx, _batchSize);
                        for (ISearchable _data : _datas) {
                            Document _doc = __doIndexDocumentCreate(_data);
                            _writer.addDocument(_doc);
                            handler.onIndexCreated(_data);
                        }
                    }
                } catch (Exception e) {
                    _LOG.info("Index Rebuilding For " + name + " Error:", RuntimeUtils.unwrapThrow(e));
                } finally {
                    __indexHelper.__doSetBuildWorking(name, false);
                }
            }

        });
    }

    /**
     * @param searchableClass 索引模型类对象
     * @return 返回索引模型类的元数据描述对象，若不存在则尝试创建
     */
    public static IndexedMeta getIndexedMeta(Class<? extends ISearchable> searchableClass) {
        __doCheckModuleInited();
        IndexedMeta _meta = __cacheIndexedMetas.get(searchableClass);
        if (_meta == null) {
            _meta = new IndexedMeta(searchableClass);
            __cacheIndexedMetas.put(searchableClass, _meta);
        }
        return _meta;
    }

    /**
     * @param searchable 索引模型类实例对象
     * @return 返回索引模型类的元数据描述对象，若不存在则尝试创建
     */
    private static IndexedMeta getIndexedMeta(ISearchable searchable) {
        return getIndexedMeta(searchable.getClass());
    }

    /**
     * @param searchable 索引模型对象
     * @return 将索引模型对象转换成Document对象
     */
    private static Document __doIndexDocumentCreate(ISearchable searchable) {
        IndexedMeta _meta = null;
        if (searchable == null || (_meta = getIndexedMeta(searchable)) == null || _meta.isEmpty()) {
            return null;
        }
        ClassBeanWrapper<?> _wrapper = ClassUtils.wrapper(searchable);
        Document _doc = new Document();
        _doc.add(new StoredField(IndexedMeta.TARGET_CLASS_NAME, searchable.getClass().getName()));
        for (String _fName : _meta.getFieldNames()) {
            if (IndexedMeta.FIELD_ID.equals(_fName)) {
                _doc.add(new StringField(IndexedMeta.FIELD_ID, new BlurObject(_wrapper.getValue(IndexedMeta.FIELD_ID)).toStringValue(), _meta.getIndexField(IndexedMeta.FIELD_ID).isStore() ? Store.YES : Store.NO));
                continue;
            }
            __doProcessDocumentField(_doc, _wrapper, _fName, _meta.getIndexField(_fName));
        }
        return _doc;
    }

    private static FieldType __doBuildFieldType(IndexField idxField) {
        FieldType _t = new FieldType();
        _t.setIndexed(true);
        _t.setOmitNorms(idxField.boost() > 1.0f);
        _t.setTokenized(idxField.isAnalyzed());
        _t.setStored(idxField.isStore());
        return _t;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void __doProcessDocumentField(Document document, ClassBeanWrapper<?> wrapper, String fieldName, IndexField idxField) {
        BlurObject _fieldValue = new BlurObject(wrapper.getValue(fieldName));
        if (_fieldValue.toObjectValue() == null) {
            return;
        }
        Class<?> _fieldType = wrapper.getFieldType(fieldName);
        Field _field = null;
        if (Date.class.equals(_fieldType)) {
            Date _date = (Date) _fieldValue.toObjectValue(Date.class);
            _field = new LongField(fieldName, _date.getTime(), idxField.isStore() ? Store.YES : Store.NO);
        } else if (Timestamp.class.equals(_fieldType)) {
            Timestamp _tstamp = (Timestamp) _fieldValue.toObjectValue(Timestamp.class);
            _field = new LongField(fieldName, _tstamp.getTime(), idxField.isStore() ? Store.YES : Store.NO);
        } else if (List.class.equals(_fieldType) || Set.class.equals(_fieldType) || Queue.class.equals(_fieldType)) {
            Collection<?> coll = (Collection<?>) _fieldValue.toObjectValue();
            StringBuilder _sb = new StringBuilder();
            for (Object o : coll) {
                _sb.append(o).append(";");
            }
            _field = new Field(fieldName, _sb.toString(), __doBuildFieldType(idxField));
        } else if (Map.class.equals(_fieldType)) {
            Map map = (Map) _fieldValue.toObjectValue();
            StringBuilder _sb = new StringBuilder();
            Set<Entry> set = map.entrySet();
            for (Entry entry : set) {
                _sb.append(entry.getValue()).append(";");
            }
            _field = new Field(fieldName, _sb.toString(), __doBuildFieldType(idxField));
        } else {
            _field = new Field(fieldName, _fieldValue.toStringValue(), __doBuildFieldType(idxField));
        }
        if (_field != null) {
            if (_field.fieldType().indexed() && _field.fieldType().omitNorms()) {
                _field.setBoost(idxField.boost());
            }
            document.add(_field);
        }
    }

}
