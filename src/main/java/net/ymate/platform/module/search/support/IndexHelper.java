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

import net.ymate.platform.commons.util.RuntimeUtils;
import net.ymate.platform.module.search.ISearchConfig;
import net.ymate.platform.module.search.Searchs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author 刘镇 (suninformation@163.com) on 15-2-7 上午10:01
 * @version 1.0
 */
public class IndexHelper {

    private static final Log _LOG = LogFactory.getLog(IndexHelper.class);

    // 标识当前是否正在执行重新打开索引文件操作
    private boolean __isWorking;

    private Set<String> __isBuildWorkingSet;

    private ScheduledExecutorService __scheduler;

    public IndexHelper(ISearchConfig config) {
        __isBuildWorkingSet = Collections.synchronizedSet(new HashSet<String>());
        // 根据配置计算任务执行间隔时间，默认30秒
        long _period = config.getScheduledPeriod() * 1000L;
        if (_period <= 0) {
            _period = 30L * 1000L;
        }
        // 创建定时任务，用于定时commit相关索引内容变更并Reopen索引文件
        __scheduler = Executors.newSingleThreadScheduledExecutor();
        // 启动定时任务服务
        __scheduler.scheduleAtFixedRate(new Runnable() {

            public void run() {
                if (__isWorking) {
                    return;
                }
                __isWorking = true;
                try {
                    _LOG.debug("Start Reopen Working...");
                    for (Map.Entry<String, IndexSearcher> entry : Searchs.__SEARCH_CACHES.entrySet()) {
                        IndexReader _reader = entry.getValue().getIndexReader();
                        try {
                            IndexReader _reOpenedReader = DirectoryReader.openIfChanged((DirectoryReader) _reader);
                            if (_reOpenedReader != null && _reOpenedReader != _reader) {
                                _reader.decRef();
                                Searchs.__SEARCH_CACHES.put(entry.getKey(), new IndexSearcher(_reOpenedReader));
                            }
                        } catch (IOException ex) {
                            _LOG.error("Reopen And DecRef IndexReader Error:", ex);
                        }
                    }
                } finally {
                    _LOG.debug("End Reopen Working...");
                    __isWorking = false;
                }
            }
        }, _period, _period, TimeUnit.MILLISECONDS);
    }
    
    public void release() {
        Searchs.__doStopSafed(__scheduler);
        //
        _LOG.debug("Release IndexHelper");
        for (IndexWriter writer : Searchs.__WRITER_CACHES.values()) {
            if (writer != null) {
                Directory dir = writer.getDirectory();
                try {
                    writer.commit();
                    writer.close(true);
                } catch (Exception ex) {
                    _LOG.error("Commit And Close IndexWriter Error", RuntimeUtils.unwrapThrow(ex));
                } finally {
                    try {
                        if (dir != null && IndexWriter.isLocked(dir)) {
                            IndexWriter.unlock(dir);
                        }
                    } catch (IOException ex) {
                        _LOG.error("Unlock IndexWriter", RuntimeUtils.unwrapThrow(ex));
                    }
                }
            }
        }
    }

    public boolean __doCheckBuildWorking(String name) {
        return __isBuildWorkingSet.contains(name);
    }

    public void __doSetBuildWorking(String name, boolean working) {
        if (working) {
            __isBuildWorkingSet.add(name);
        } else {
            __isBuildWorkingSet.remove(name);
        }
    }

}
